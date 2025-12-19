package internal

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"reflect"
	"runtime"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/oklog/ulid"
	"github.com/oklog/ulid/v2"

	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/protoimpls/wsgorilla"
)

const localConfig = `
exporters:
  otlp:
    endpoint: localhost:1111

receivers:
  otlp:
    protocols:
      grpc: {}
      http: {}

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: []
      exporters: [otlp]
`

type reportEffectiveConfig int

const (
	reportEffectiveConfigDontReport reportEffectiveConfig = iota
	reportEffectiveConfigHashOnly
	reportEffectiveConfigFull
)

type Agent struct {
	logger *log.Logger

	agentType    string
	agentVersion string

	dataDir   string
	addonsDir string

	effectiveConfig     string
	effectiveConfigHash []byte
	//needReportEffectiveConfig reportEffectiveConfig

	//remoteConfigStatus protobufs.RemoteConfigStatus
	nextMessage protobufs.AgentToServer

	upSince time.Time

	instanceId ulid.ULID

	agentDescription *protobufs.AgentDescription

	opampClient *wsgorilla.Client

	remoteConfigHash []byte
	allAddonHash     []byte
	//addonStatuses      protobufs.AgentAddonStatuses
	//agentInstallStatus protobufs.AgentInstallStatus

	opampClientCert *tls.Certificate

	// Errors message that resulted from trying to use offered certificates.
	//clientCertOfferErrors map[protobufs.ClientCertFor]string

	// OTLP destination to send own metrics to
	metricDestination *protobufs.ConnectionSettings
	// Optional client cert to use with destination.
	metricClientCert *tls.Certificate
	metricReporter   *MetricReporter
}

func NewAgent(dataDir string, logger *log.Logger, agentType string, agentVersion string) *Agent {
	agent := &Agent{
		effectiveConfig: localConfig,
		dataDir:         dataDir,
		logger:          logger,
		agentType:       agentType,
		agentVersion:    agentVersion,
		upSince:         time.Now(),
		//needReportEffectiveConfig: reportEffectiveConfigHashOnly,
	}

	agent.createAgentId()
	agent.logger.Printf(
		"Agent starting, id=%v, type=%s, version=%s...\n",
		agent.instanceId.String(), agentType, agentVersion,
	)

	agent.parseLocalConfig()
	if err := agent.connect(); err != nil {
		log.Fatalf("Cannot connect to OpAMP server: %v\n", err)
	}

	return agent
}

func (agent *Agent) connect() error {
	agent.logger.Println("Connecting to server...")
	agent.opampClient = &wsgorilla.Client{
		OnRemoteData: agent.onMsgFromServer,
		UseTSL:       true,
		ClientCert:   agent.opampClientCert,
		CertDir:      path.Join(agent.dataDir, "../../../../../internal/certs"),
	}
	agent.opampClient.SetInstanceId(agent.instanceId.String())
	agent.opampClient.SetAuthorizationHeader("Bearer 12345678")
	err := agent.opampClient.Connect("127.0.0.1:4320")
	if err != nil {
		return err
	}
	agent.logger.Println("Connected to server.")

	agent.reportInitialStatus()

	return nil
}

func (agent *Agent) disconnect() {
	agent.logger.Println("Disconnecting from server...")
	agent.opampClient.Shutdown()
}

func (agent *Agent) createAgentId() {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(0)), 0)
	agent.instanceId = ulid.MustNew(ulid.Timestamp(time.Now()), entropy)

	agent.addonsDir = path.Join(agent.dataDir, "addons")
	err := os.MkdirAll(agent.addonsDir, 0o755)
	if err != nil {
		log.Fatalf("Cannot create direectory %s: %v\n", agent.addonsDir, err)
	}

	hostname, _ := os.Hostname()

	agent.agentDescription = &protobufs.AgentDescription{
		AgentType:    agent.agentType,
		AgentVersion: agent.agentVersion,
		AgentAttributes: []*protobufs.KeyValue{
			{
				Key: "os.family",
				Value: protobufs.AnyValue{
					Value: &protobufs.AnyValue_StringValue{
						StringValue: runtime.GOOS,
					},
				},
			},
			{
				Key: "host.name",
				Value: protobufs.AnyValue{
					Value: &protobufs.AnyValue_StringValue{
						StringValue: hostname,
					},
				},
			},
		},
	}
}

func (agent *Agent) initMeter() {
	reporter, err := NewMetricReporter(agent.logger, agent.metricDestination, agent.metricClientCert, agent)
	if err != nil {
		agent.logger.Printf("Cannot collect metrics: %v", err)
		return
	}

	prevReporter := agent.metricReporter

	agent.metricReporter = reporter

	if prevReporter != nil {
		prevReporter.Shutdown()
	}

	return
}

func (agent *Agent) parseLocalConfig() {
	var k = koanf.New(".")
	k.Load(rawbytes.Provider([]byte(localConfig)), yaml.Parser())

	effectiveConfigBytes, err := k.Marshal(yaml.Parser())
	if err != nil {
		panic(err)
	}

	agent.effectiveConfig = string(effectiveConfigBytes)
	hash := sha256.Sum256(effectiveConfigBytes)
	agent.effectiveConfigHash = hash[:]
}

func (agent *Agent) onMsgFromServer(msg *protobufs.ServerToAgent) {
	agent.logger.Printf("<- Received\n")

	if msg.RemoteConfig != nil {
		err := agent.applyRemoteConfig(msg.RemoteConfig)
		remoteConfigStatus := &protobufs.RemoteConfigStatus{
			LastRemoteConfigHash: msg.RemoteConfig.ConfigHash,
		}
		if err != nil {
			remoteConfigStatus.Status = protobufs.RemoteConfigStatus_Failed
			remoteConfigStatus.ErrorMessage = err.Error()

		} else {
			remoteConfigStatus.Status = protobufs.RemoteConfigStatus_Applied
		}
		if agent.nextMessage.StatusReport == nil {
			agent.nextMessage.StatusReport = &protobufs.StatusReport{
				AgentDescription: agent.agentDescription,
			}
		}
		agent.nextMessage.StatusReport.RemoteConfigStatus = remoteConfigStatus
	}

	if msg.ConnectionSettings != nil {
		agent.applyConnectionSettings(msg.ConnectionSettings)
	}

	addons := msg.GetAddonsAvailable()
	if addons != nil {
		forceStatusReport := (msg.Flags & protobufs.ServerToAgent_ReportAddonStatus) != 0
		if forceStatusReport {
			agent.logger.Printf("Server asked to report addon status.\n")
		}
		agent.applyAddonsAvailable(addons, forceStatusReport)
	}

	agentPackage := msg.GetAgentPackageAvailable()
	if agentPackage != nil {
		agent.applyAgentPackageAvailable(agentPackage)
	}

	if (msg.Flags & protobufs.ServerToAgent_ReportEffectiveConfig) != 0 {
		agent.logger.Printf("Server asked to report effective config.\n")
		agent.reportEffectiveConfig()
	}

	agent.sendNextMessage()
}

func (agent *Agent) applyConnectionSettings(msg *protobufs.ConnectionSettingsOffers) {
	agent.logger.Println("Received connection settings from server.")

	if msg.Opamp != nil {
		agent.applyOpampConnectionSettings(msg.Opamp)
	}

	if msg.OwnMetrics != nil {
		agent.metricDestination = msg.OwnMetrics
		agent.initMeter()
	}

}

func (agent *Agent) getCertInSettings(certificate *protobufs.TLSCertificate) (*tls.Certificate, error) {
	cert, err := tls.X509KeyPair(
		certificate.PublicKey,
		certificate.PrivateKey,
	)
	if err != nil {
		agent.logger.Printf("Received invalid certificate offer: %s\n", err)
		return nil, err
	}

	if len(certificate.CaPublicKey) != 0 {
		caCertPB, _ := pem.Decode(certificate.CaPublicKey)
		caCert, err := x509.ParseCertificate(caCertPB.Bytes)
		if err != nil {
			agent.logger.Printf("Cannot parse CA cert: %v", err)
			return nil, err
		}
		agent.logger.Printf("Received offer signed by CA: %v", caCert.Subject)
	}

	return &cert, nil
}

func (agent *Agent) applyOpampConnectionSettings(
	settings *protobufs.ConnectionSettings,
) {

	if settings.Certificate == nil {
		agent.logger.Printf("Received nil certificate offer, ignoring.\n")
	}

	cert, err := agent.getCertInSettings(settings.Certificate)
	if err != nil {
	}

	agent.logger.Printf("Reconnecting to verify offered client certificate.\n")

	agent.disconnect()
	agent.opampClientCert = cert
	if err := agent.connect(); err != nil {
		agent.logger.Printf("Cannot connect using offered certificate: %s. Ignoring the offer\n", err)
		agent.opampClientCert = nil

		if err := agent.connect(); err != nil {
			agent.logger.Printf("Unable to reconnect after restoring client certificate: %v\n", err)
		}
	}

	agent.logger.Printf("Successfully connected to server. Accepting new client certificate.\n")
}

func getStructValTypeName(val interface{}) string {
	if t := reflect.TypeOf(val); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

func (agent *Agent) sendNextMessage() {
	if proto.Equal(&agent.nextMessage, &protobufs.AgentToServer{}) {
		// Nothing to send, no fields are set.
		return
	}

	agent.logger.Printf(
		"-> Sending message\n",
	)
	agent.opampClient.SendMessage(&agent.nextMessage)
	agent.nextMessage = protobufs.AgentToServer{}
}

func (agent *Agent) reportInitialStatus() {
	status := &protobufs.StatusReport{
		AgentDescription: agent.agentDescription,
	}

	status.EffectiveConfig = &protobufs.EffectiveConfig{
		Hash: agent.effectiveConfigHash,
		ConfigMap: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"": {Body: []byte(agent.effectiveConfig)},
			},
		},
	}

	agent.logger.Printf("-> Sending initial status report\n")
	agent.opampClient.SendMessage(
		&protobufs.AgentToServer{
			StatusReport: status,
			AddonStatuses: &protobufs.AgentAddonStatuses{
				ServerProvidedAllAddonsHash: agent.allAddonHash,
			},
		},
	)
}

type AgentConfigFileItem struct {
	name string
	file *protobufs.AgentConfigFile
}

type AgentConfigFileSlice []AgentConfigFileItem

func (a AgentConfigFileSlice) Less(i, j int) bool {
	return a[i].name < a[j].name
}

func (a AgentConfigFileSlice) Swap(i, j int) {
	t := a[i]
	a[i] = a[j]
	a[j] = t
}

func (a AgentConfigFileSlice) Len() int {
	return len(a)
}

func (agent *Agent) applyRemoteConfig(config *protobufs.AgentRemoteConfig) error {
	agent.logger.Printf("Received remote config from server, hash=%x.\n", config.ConfigHash)

	var k = koanf.New(".")
	if err := k.Load(rawbytes.Provider([]byte(localConfig)), yaml.Parser()); err != nil {
		return err
	}

	orderedConfigs := AgentConfigFileSlice{}
	for name, file := range config.Config.ConfigMap {
		if name == "" {
			// skip instance config
			continue
		}
		orderedConfigs = append(
			orderedConfigs, AgentConfigFileItem{
				name: name,
				file: file,
			},
		)
	}

	sort.Sort(orderedConfigs)

	// Append instance config as the last item.
	instanceConfig := config.Config.ConfigMap[""]
	if instanceConfig != nil {
		orderedConfigs = append(
			orderedConfigs, AgentConfigFileItem{
				name: "",
				file: instanceConfig,
			},
		)
	}

	for _, item := range orderedConfigs {
		var k2 = koanf.New(".")
		err := k2.Load(rawbytes.Provider(item.file.Body), yaml.Parser())
		if err != nil {
			return fmt.Errorf("cannot parse config named %s: %v", item.name, err)
		}
		err = k.Merge(k2)
		if err != nil {
			return fmt.Errorf("cannot merge config named %s: %v", item.name, err)
		}
	}

	effectiveConfigBytes, err := k.Marshal(yaml.Parser())
	if err != nil {
		panic(err)
	}

	newEffectiveConfig := string(effectiveConfigBytes)
	if agent.effectiveConfig != newEffectiveConfig {
		agent.logger.Printf("Effective config changed. Need to report to server.\n")
		agent.effectiveConfig = newEffectiveConfig
		hash := sha256.Sum256(effectiveConfigBytes)
		agent.effectiveConfigHash = hash[:]
		agent.reportEffectiveConfig()
	}

	agent.remoteConfigHash = config.ConfigHash

	return nil
}

func (agent *Agent) reportEffectiveConfig() {
	msg := &protobufs.EffectiveConfig{
		Hash: agent.effectiveConfigHash,
		ConfigMap: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"": {Body: []byte(agent.effectiveConfig)},
			},
		},
	}

	if agent.nextMessage.StatusReport == nil {
		agent.nextMessage.StatusReport = &protobufs.StatusReport{
			AgentDescription: agent.agentDescription,
		}
	}
	agent.nextMessage.StatusReport.EffectiveConfig = msg
}

//func (agent *Agent) reportLocalAddonStatuses() {
//	// Read the list of addons we have locally.
//	localAddons, err := ioutil.ReadDir(agent.addonsDir)
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	addonStatuses := protobufs.AgentAddonStatuses{}
//	addonStatuses.Addons = map[string]*protobufs.AgentAddonStatus{}
//	for _, addonDir := range localAddons {
//		if !addonDir.IsDir() {
//			continue
//		}
//		addonDirPath := path.Join(agent.addonsDir, addonDir.Name())
//
//		addonFiles, err := ioutil.ReadDir(addonDirPath)
//		if err != nil {
//			log.Fatalln(err)
//		}
//
//		status := &protobufs.AgentAddonStatus{
//			Name:   addonDir.Name(),
//			Status: protobufs.AgentAddonStatus_INSTALLED,
//		}
//
//		// TODO: instead of re-calculating the hashes, store the hashes locally
//		// and use them here. That's what the protocol requires.
//		addonSha256 := sha256.New()
//		addonSha256.Write([]byte(status.Name))
//
//		for _, addonFile := range addonFiles {
//			if addonFile.IsDir() {
//				continue
//			}
//
//			filePath := path.Join(addonDirPath, addonFile.Name())
//			sha256, err := common.CalcFilePathSha256(filePath)
//			if err != nil {
//				status.ErrorMessage = err.Error()
//			}
//
//			addonSha256.Write(sha256)
//			addonSha256.Write([]byte(addonFile.Name()))
//		}
//		status.AgentHasHash = addonSha256.Sum(nil)
//		addonStatuses.Addons[status.Name] = status
//	}
//	agent.reportAddonStatuses(&addonStatuses)
//}

func (agent *Agent) SendMessage(msg *protobufs.AgentToServer) error {
	agent.logger.Printf("-> Sending\n")
	return agent.opampClient.SendMessage(msg)
}

func (agent *Agent) reportAddonStatuses(addons *protobufs.AgentAddonStatuses) {
	agent.logger.Printf("Reporting addon status\n")
	err := agent.SendMessage(
		&protobufs.AgentToServer{
			AddonStatuses: addons,
		},
	)
	if err != nil {
		agent.logger.Printf("Cannot report addon statuses: %v\n", err)
	}
}

func (agent *Agent) applyAddonsAvailable(addons *protobufs.AddonsAvailable, forceStatusReport bool) {
	agent.logger.Printf("Received addons list from server, hash=%x\n", addons.AllAddonsHash)

	currentHash := agent.allAddonHash
	if forceStatusReport {
		// To force AddonsSyncer to generate a status report we give a nil current hash.
		currentHash = nil
	}

	syncer := NewAddonsSyncer(agent.addonsDir, currentHash, agent.logger)
	syncer.Sync(addons)

	if syncer.AddonsChanged || forceStatusReport ||
		bytes.Compare(agent.allAddonHash, syncer.AddonStatuses.ServerProvidedAllAddonsHash) != 0 {
		// If during sync any addons changed or if our hash was initially different
		// from the final hash after sync, report the status.
		agent.nextMessage.AddonStatuses = &syncer.AddonStatuses
	}

	agent.allAddonHash = syncer.AddonStatuses.ServerProvidedAllAddonsHash
}

func (agent *Agent) applyAgentPackageAvailable(agentPackage *protobufs.AgentPackageAvailable) bool {
	agent.logger.Println("Received agent package from server.")

	// For demo purposes we just set install status to failed immediately. Real OpAMP
	// client should try to verify the package and install it.
	agentInstallStatus := protobufs.AgentInstallStatus{
		ServerOfferedVersion: agentPackage.Version,
		ServerOfferedHash:    agentPackage.File.ContentHash,
		Status:               protobufs.AgentInstallStatus_InstallFailed,
		ErrorMessage:         "Cannot verify signature of the package",
	}
	agent.nextMessage.AgentInstallStatus = &agentInstallStatus
	return true
}

func (agent *Agent) Shutdown() {
	agent.logger.Println("Agent shutting down...")
	if agent.metricReporter != nil {
		agent.metricReporter.Shutdown()
	}
}
