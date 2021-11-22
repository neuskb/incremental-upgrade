package increupgrade

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sort"
)

/*
upgrade
|
|--version.json
|
|--script
	|
	|--V1.0.0_20211111_standard.sh
	|
	|--V1.0.0_20211111_standard.sql
	|
	|--V1.0.0_20211112_standard.sql
	|
	|--...
*/

type IncreUpgradeEngine struct {
	CurVerType string
	SrcVersion string
	DstVersion string
	DbPath     string
}

func (increUp *IncreUpgradeEngine) SetVerionInfo(curVerType, srcVersion, dstVersion, dbPath string) {
	increUp.CurVerType = curVerType
	increUp.SrcVersion = srcVersion
	increUp.DstVersion = dstVersion
	increUp.DbPath = dbPath
}

func (increUp *IncreUpgradeEngine) GetSortVersions() ([]string, map[string]interface{}) {
	verFile := "./upgrade/version.json"
	if !fileExist(verFile) {
		log.Printf("[GetSortVersions] err: can not find version.json \n")
		return nil, nil
	}

	jsonFile, err := os.Open(verFile)
	if err != nil {
		log.Printf("[GetSortVersions] open version.json failed: %s \n", err)
		return nil, nil
	}
	defer jsonFile.Close()

	bytesData, _ := ioutil.ReadAll(jsonFile)

	var result map[string]interface{}
	json.Unmarshal([]byte(bytesData), &result)

	var sortVersions []string
	for k := range result {
		sortVersions = append(sortVersions, k)
	}
	sort.Strings(sortVersions)

	return sortVersions, result
}

func (increUp *IncreUpgradeEngine) DoSelectAndExec(result map[string]interface{}, scriptVerion string) bool {
	for versionType, value := range result[scriptVerion].(map[string]interface{}) {
		log.Printf("[DoSelectAndExec] versionType: %s \n", versionType)
		// 只有json中匹配当前版本的升级规则才会执行
		// 可以支持不同支线版本升级
		if versionType == increUp.CurVerType {
			for scriptType, script := range value.(map[string]interface{}) {
				log.Printf("[DoSelectAndExec] scriptType: %s script: %s \n", scriptType, script)
				command := "./upgrade/script/" + script.(string)
				log.Println("[DoSelectAndExec] command:", command)
				if err := increUp.ExecUpgradeScript(scriptType, increUp.DbPath, command); !err {
					log.Printf("[DoSelectAndExec] ExecUpgradeScript failed \n")
					//暂时不返回错误，只提示哪个脚本执行出错，确保升级成功
					//return false
				}
			}
		}
	}

	return true
}

func (increUp *IncreUpgradeEngine) IncreUpgrade(curVerType, srcVersion, dstVersion, dbPath string) bool {
	increUp.SetVerionInfo(curVerType, srcVersion, dstVersion, dbPath)

	sortVersions, result := increUp.GetSortVersions()
	if sortVersions == nil || result == nil {
		return false
	}

	for _, scriptVerion := range sortVersions {
		log.Printf("[IncreUpgrade] scriptVersion: %s srcVersion: %s dstVersion: %s \n", scriptVerion, srcVersion, dstVersion)
		// 增量升级，[srcVersion,dstVersion] 之间的sql/sh 才会执行
		if scriptVerion < srcVersion || scriptVerion > dstVersion {
			log.Printf("[IncreUpgrade] script version < curVersion, not need exec upgrade \n")
			continue
		}

		increUp.DoSelectAndExec(result, scriptVerion)
	}

	log.Printf("[IncreUpgrade] success! \n")
	return true
}

func (increUp *IncreUpgradeEngine) ExecUpgradeScript(scriptType, dbPath, command string) bool {
	var out bytes.Buffer
	var stderr bytes.Buffer
	var cmd *exec.Cmd
	if scriptType == "sql" {
		sqlCommand := ".read " + command
		log.Printf("[ExecUpgradeScript] sqlCommand: %s", sqlCommand)
		cmd = exec.Command("sqlite3", dbPath, sqlCommand)
		cmd.Stdout = &out
		cmd.Stderr = &stderr
	} else if scriptType == "sh" {
		cmd = exec.Command(command)
		cmd.Stdout = &out
		cmd.Stderr = &stderr
	} else {
		log.Printf("[ExecUpgradeScript] not support scriptType: %s", scriptType)
		return false
	}

	err := cmd.Run()
	if err != nil {
		log.Printf("execUpgradeScript cmd: %s, err: %v, stderr: %s", command, err, stderr.String())
		return false
	}

	return true
}

func fileExist(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
