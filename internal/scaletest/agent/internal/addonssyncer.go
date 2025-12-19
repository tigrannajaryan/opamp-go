package internal

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"

	"github.com/open-telemetry/opamp-go/internal/scaletest/common"
	"github.com/open-telemetry/opamp-go/protobufs"
)

type AddonsSyncer struct {
	localAddonsDir string

	// Is set to true by Sync() if any changes to local addons are made.
	AddonsChanged bool

	// Contains the local status of addons after Sync() call.
	AddonStatuses protobufs.AgentAddonStatuses

	localAllAddonHash []byte

	logger *log.Logger
}

func NewAddonsSyncer(localAddonsDir string, localAllAddonHash []byte, logger *log.Logger) *AddonsSyncer {
	return &AddonsSyncer{
		localAddonsDir:    localAddonsDir,
		AddonsChanged:     false,
		localAllAddonHash: localAllAddonHash,
		AddonStatuses: protobufs.AgentAddonStatuses{
			ServerProvidedAllAddonsHash: localAllAddonHash,
		},
		logger: logger,
	}
}

// Sync synchronizes addons from the server to the local addons directory.
func (s *AddonsSyncer) Sync(addons *protobufs.AddonsAvailable) {
	s.AddonsChanged = false

	if bytes.Compare(s.localAllAddonHash, addons.AllAddonsHash) == 0 {
		s.logger.Println("All addons are already up to date.")
		return
	}

	s.AddonStatuses.Addons = map[string]*protobufs.AgentAddonStatus{}

	// Iterate through offered addons and sync them all from server.
	for name, addon := range addons.Addons {
		s.syncAddon(name, addon)
	}

	s.deleteUneededLocalAddons(addons)

	// Remember the aggregate hash for fast check next time.
	s.AddonStatuses.ServerProvidedAllAddonsHash = addons.AllAddonsHash

	s.logger.Println("All addons are synced and up to date.")
}

func (s *AddonsSyncer) syncAddon(addonName string, addon *protobufs.AddonAvailable) {
	addonSubdir := path.Join(s.localAddonsDir, addonName)

	// TODO: store addon hash locally, compare with what we received from server
	// and skip the entire addon if the hashes match.

	// Prepare status for this addon.
	status := &protobufs.AgentAddonStatus{
		Name:              addonName,
		AgentHasHash:      addon.Hash,
		ServerOfferedHash: addon.Hash,
		Status:            protobufs.AgentAddonStatus_Installed,
	}

	s.AddonStatuses.Addons[status.Name] = status

	// Make sure addon subdirectory exists.
	err := os.MkdirAll(addonSubdir, 0o755)
	if err != nil {
		err = fmt.Errorf("cannot create addon directory %s: %v", addonSubdir, err)
		s.logger.Println(err)
		status.Status = protobufs.AgentAddonStatus_InstallFailed
		status.ErrorMessage = err.Error()
		s.AddonsChanged = true
		return
	}

	// Iterate over addon files and ensure they exists or download them.
	fileName := "content"
	err = s.syncAddonFile(addonName, addonSubdir, fileName, addon.File)
	if err != nil {
		status.Status = protobufs.AgentAddonStatus_InstallFailed
		status.ErrorMessage = err.Error()
		s.AddonsChanged = true
	}

	//s.deleteUneededLocalFiles(addonSubdir, addon)
}

func (s *AddonsSyncer) syncAddonFile(
	addonName string,
	addonSubdir string,
	addonFileName string,
	addonFile *protobufs.DownloadableFile,
) error {
	addonFilePath := path.Join(addonSubdir, addonFileName)

	shouldDownload, err := s.shouldDownloadAddonFile(addonName, addonFileName, addonFile, addonFilePath)
	if err == nil && shouldDownload {
		err = s.downloadFile(addonFile.DownloadUrl, addonFilePath)
		s.AddonsChanged = true
	}

	return err
}

func (s *AddonsSyncer) deleteUneededLocalAddons(serverAddons *protobufs.AddonsAvailable) {
	// Read the list of addons we have locally.
	localAddons, err := ioutil.ReadDir(s.localAddonsDir)
	if err != nil {
		log.Fatalln(err)
	}

	for _, locaAddon := range localAddons {
		// Do we have an addon that is not offered?
		if _, offered := serverAddons.Addons[locaAddon.Name()]; !offered {
			s.logger.Printf("Addon %s is no longer needed, deleting.\n", locaAddon.Name())
			err = os.RemoveAll(path.Join(s.localAddonsDir, locaAddon.Name()))
			s.AddonsChanged = true
			if err != nil {
				log.Fatalln(err)
			}
		}
	}
}

//func (s *AddonsSyncer) deleteUneededLocalFiles(addonDir string, serverAddon *protobufs.AddonAvailable) {
//	// Read the list of addons we have locally.
//	localFiles, err := ioutil.ReadDir(addonDir)
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	for _, locaFile := range localFiles {
//		// Do we have an addon that is not offered?
//		if _, offered := serverAddon.Files.Files[locaFile.Name()]; !offered {
//			s.logger.Printf("Addon file %s is no longer needed, deleting.\n", locaFile.Name())
//			err = os.RemoveAll(path.Join(addonDir, locaFile.Name()))
//			s.AddonsChanged = true
//			if err != nil {
//				log.Fatalln(err)
//			}
//		}
//	}
//}

func (s *AddonsSyncer) shouldDownloadAddonFile(
	addonName string,
	addonFileName string,
	addonFile *protobufs.DownloadableFile,
	addonFilePath string,
) (bool, error) {
	if _, err := os.Stat(addonFilePath); err == nil {
		// The file exists locally. Calculate file checksum.
		sha256, err := common.CalcFilePathSha256(addonFilePath)

		if err != nil {
			err := fmt.Errorf("cannot calculate checksum of %s: %v", addonFilePath, err)
			s.logger.Println(err)
			return false, err
		} else {
			// Compare the checksum of the file we have with what
			// we are offered by the server.
			if bytes.Compare(sha256, addonFile.ContentHash) != 0 {
				s.logger.Printf("Addon %s, file %s checksum mismatch, will download.\n",
					addonName, addonFileName)
				return true, nil
			}
		}
	} else {
		s.logger.Printf("Addon %s, file %s does not exist, will download.\n",
			addonName, addonFileName)
		return true, nil
	}
	return false, nil
}

func (s *AddonsSyncer) downloadFile(url string, toFilePath string) error {
	s.logger.Printf("Downloading from %s to %s\n", url, toFilePath)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("cannot download file %s from %s: %v", url, toFilePath, err)
	}
	defer resp.Body.Close()

	file, err := os.Create(toFilePath)
	if err != nil {
		return fmt.Errorf("cannot create file %s: %v", toFilePath, err)
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("cannot download file %s from %s: %v", url, toFilePath, err)
	}
	return nil
}
