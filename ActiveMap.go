package active

import (
	"fmt"
	"io/ioutil"
	"log"
	//"math"
	//"net"
	//"sort"
	"strings"
	//"strconv"
	"bufio"
	"github.com/go-while/go-utils"
	"github.com/go-while/nntp-config"
	"github.com/go-while/nntp-overview"
	"os"
	"sort"
	"sync"
	//"time"
)

var (
	BIGNUM31 uint64 = 2147483647
	BIGNUM32 uint64 = 4294967295
	BIGNUM63 uint64 = 9223372036854775807
	BIGNUM64 uint64 = 18446744073709551615
	BIGNUM_DEFAULT = BIGNUM31
	BIGNUM = BIGNUM_DEFAULT
	//BIGNUM64 uint64 = uint64(math.Pow(2, 64)) - 1
	DebugActiveMap bool = false
)

var (
	lock_write_active_map = make(chan struct{}, 1)
)

type ActiveData struct {
	Group  string
	Num    uint64
	Hi     uint64
	Lo     uint64
	Status string
	Update int64
	Hash   string
} // end type ActiveData

// ActiveMap is safe to use concurrently.
type ActiveMap struct {
	V                     map[string]ActiveData /// key group, val ActiveData
	HashmapH              map[string]string     /// key hash, val group
	HashmapG              map[string]string     /// key group, val hash
	mux                   sync.RWMutex
	Hashmux               sync.RWMutex // Hashmux
	Lock_write_active_map chan struct{}
}

func (c *ActiveMap) BootActive() {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.V = make(map[string]ActiveData)
	c.HashmapG = make(map[string]string)
	c.HashmapH = make(map[string]string)
	c.Lock_write_active_map = make(chan struct{}, 1)
	c.Lock_write_active_map <- struct{}{} // fill with 1 lock
}

func (c *ActiveMap) SetActiveData(group string, data ActiveData) bool {

	if group == "" {
		log.Printf("ERROR setActiveMap group empty")
		return false
	}
	if DebugActiveMap {
		//log.Printf("setActiveMap group='%s' data='%v'", group, data)
	}

	if data.Hash == "" {
		data.Hash = utils.Hash256(group)
	}

	c.mux.Lock()
	c.V[group] = data
	c.mux.Unlock()

	c.Hashmux.Lock()
	c.HashmapH[data.Hash] = data.Group
	c.HashmapG[data.Group] = data.Hash
	c.Hashmux.Unlock()

	return true
} // end func setActiveData

func (c *ActiveMap) UpActiveValue(group string, key string) {

	if group == "" {
		log.Printf("ERROR upActiveValue group empty")
		return
	}


	c.mux.Lock()
	data := c.V[group]

	if data.Hash == "" {
		data.Hash = utils.Hash256(group)
	}
	if data.Group == "" {
		data.Group = group
	}
	if data.Status == "" {
		data.Status = "y"
	}
	data.Update = utils.UnixTimeSec()
	switch key {
	case "hi":
		data.Hi++
		data.Num++
	}
	c.V[group] = data
	if DebugActiveMap {
		log.Printf("upActiveValue group='%s' key='%v' num=%d hi=%d", group, key, data.Num, data.Hi)
	}
	c.mux.Unlock()
} // end func setActiveData

func (c *ActiveMap) GetActiveMap(id uint64, short bool, bignum uint64, cutLowGroups bool) []string {
	var retlist []string
	c.mux.RLock()

	len_cv := len(c.V)
	for _, data := range c.V {
		if short {
			line := data.Group
			retlist = append(retlist, line)
			continue
		}
		line := ""
		switch bignum {
		case BIGNUM31:
			if data.Hi >= BIGNUM31 {
				line = fmt.Sprintf("%s %010d %010d %s", data.Group, BIGNUM31, data.Lo, data.Status) // leftpad zeros
			} else {
				line = fmt.Sprintf("%s %010d %010d %s", data.Group, data.Hi, data.Lo, data.Status) // leftpad zeros
			}
		case BIGNUM32:
			if data.Hi >= BIGNUM32 {
				line = fmt.Sprintf("%s %010d %010d %s", data.Group, BIGNUM32, data.Lo, data.Status) // leftpad zeros
			} else {
				line = fmt.Sprintf("%s %010d %010d %s", data.Group, data.Hi, data.Lo, data.Status) // leftpad zeros
			}
		case BIGNUM63:
			if data.Hi >= BIGNUM63 {
				line = fmt.Sprintf("%s %019d %019d %s", data.Group, BIGNUM63, data.Lo, data.Status) // leftpad zeros
			} else {
				line = fmt.Sprintf("%s %019d %019d %s", data.Group, data.Hi, data.Lo, data.Status) // leftpad zeros
			}
		case BIGNUM64:
			//line = fmt.Sprintf("%s %020d %020d %s", data.Group, data.Hi, data.Lo, data.Status) // leftpad zeros
			line = fmt.Sprintf("%s %d %d %s", data.Group, data.Hi, data.Lo, data.Status)
		}
		//line := fmt.Sprintf("%s %010d %010d %s", data.Group, data.Hi, data.Lo, data.Status) // leftpad zeros
		if line != "" {
			retlist = append(retlist, line)
		}
		//log.Printf("getActiveMap line='%s'", line)
	} // end for data cv
	c.mux.RUnlock()
	log.Printf("[L=%d] getActiveMap len_cv=%d ret=%d", id, len_cv, len(retlist))
	sort.Sort(AsortFunc(retlist))
	return retlist
} // end func getActiveMap

func (c *ActiveMap) GetActiveData(group string) ActiveData {
	c.mux.RLock()
	data := c.V[group]
	c.mux.RUnlock()
	if data.Group != group {
		log.Printf("ERROR getActiveData group='%s' key!=group", group)
		return ActiveData{}
	}
	if data.Hash == "" {
		data.Hash = utils.Hash256(group)

		c.mux.Lock()
		c.V[group] = data
		c.mux.Unlock()

		c.Hashmux.Lock()
		c.HashmapH[data.Hash] = data.Group
		c.HashmapG[data.Group] = data.Hash
		c.Hashmux.Unlock()

		log.Printf("INFO getActiveData group='%s' set hash='%s'", group, data.Hash)
	}

	/*
		retdata.Hash = data.Hash
		retdata.Group = data.Group
		retdata.num = data.num
		retdata.Hi = data.Hi
		retdata.Lo = data.Lo
		retdata.Status = data.Status
		retdata.Update = data.Update
	*/

	//log.Printf("getActiveData group='%s' data='%v'", group, data)
	return data
} // end func getActiveData

func (c *ActiveMap) Find_hash(hash string) string {
	c.Hashmux.RLock()
	group := c.HashmapH[hash]
	c.Hashmux.RUnlock()
	if group != "" {
		return group
	}
	return ""

	/*
		c.mux.Lock()
		defer c.mux.Unlock()
		for group, data := range c.V {
			if data.Hash == hash && data.Group == group {
				log.Printf("find_hash=%s == group='%s'", hash, group)
				return string(group)
			}
		}
		log.Printf("ERROR find_hash not found group for hash='%s'", hash)
		return ""
	*/
}

type AsortFunc []string

func (nf AsortFunc) Len() int      { return len(nf) }
func (nf AsortFunc) Swap(i, j int) { nf[i], nf[j] = nf[j], nf[i] }
func (nf AsortFunc) Less(i, j int) bool {
	return nf[i] < nf[j]
}

type AsortFuncPTR []*string

func (nf AsortFuncPTR) Len() int      { return len(nf) }
func (nf AsortFuncPTR) Swap(i, j int) { nf[i], nf[j] = nf[j], nf[i] }
func (nf AsortFuncPTR) Less(i, j int) bool {
	return *nf[i] < *nf[j]
}

func (c *ActiveMap) Unlock_write_activemap() bool {
	log.Printf("unlock_write_activemap")
	//globalTimer.setTimer("cron:write_activemap", "s")
	//globalBools.SetGB("is_write_activemap", false)
	c.Lock_write_active_map <- struct{}{}
	return true
} // end func unlock_write_activemap

func (c *ActiveMap) Lock_write_activemap() {
	log.Printf("get Lock_write_activemap")
	<-c.Lock_write_active_map
	log.Printf("got Lock_write_activemap")
} // end func lock_write_activemap

func (c *ActiveMap) Write_activemap(cfgSettings *config.SETTINGS, ignore_notboot bool) bool {

	if !ignore_notboot {
		//isopen_server("write_activemap", -1)
	}

	if cfgSettings.ActiveDir == "" {
		log.Printf("WARN write_activemap cfgSettings.ActiveDir not set!")
		return false
	}

	c.Lock_write_activemap()
	defer c.Unlock_write_activemap()
	short := false
	cutLowGroups := false
	list := c.GetActiveMap(0, short, BIGNUM_DEFAULT, cutLowGroups)
	if len(list) == 0 {
		log.Printf("INFO write_activemap len(list)==0")
		return false
	}
	now := utils.UnixTimeNanoSec()
	local_active_file := cfgSettings.ActiveDir + "/local-mode.active"
	filename_tmp := fmt.Sprintf("%s.%d.tmp", local_active_file, now)
	filename_bak := fmt.Sprintf("%s.old", local_active_file)
	fh, err := os.OpenFile(filename_tmp, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("ERROR write_activemap tmp_file='%s'", filename_tmp)
		return false
	}

	log.Printf("Write ActiveMap len=%d tmp_file='%s'", len(list), filename_tmp)

	datawriter := bufio.NewWriter(fh)
	wrote := 0
	for _, data := range list {
		//log.Printf("write_activemap: writing list i=%d data_len=%d", i, len(data))
		n, err := datawriter.WriteString(data + "\n")
		if err != nil {
			log.Printf("ERROR write_activemap failed WriteString to .tmp file='%s' err='%v'", filename_tmp, err)
			return false
		} else {
			wrote += n
			//log.Printf("[   writeList   ]: wrote %d bytes to .tmp list-file: %s", n, filename_list)
		}
	}

	if err := datawriter.Flush(); err != nil {
		log.Printf("ERROR: write_activemap flush failed")
		return false
	}

	if err := fh.Close(); err != nil {
		log.Printf("ERROR: write_activemap close .tmp failed")
		return false
	}
	// tmp file closed ok

	// remove .2 and move .1 and .old
	if utils.FileExists(filename_bak + ".2") {
		if err := os.Remove(filename_bak + ".2"); err != nil {
			log.Printf("ERROR write_activemap remove .2 failed err='%v'", err)
			return false
		}
	}
	if utils.FileExists(filename_bak + ".1") {
		if err := os.Rename(filename_bak+".1", filename_bak+".2"); err != nil {
			log.Printf("ERROR write_activemap rename .1 to .2 failed err='%v'", err)
			return false
		}
	}
	if utils.FileExists(filename_bak) {
		if err := os.Rename(filename_bak, filename_bak+".1"); err != nil {
			log.Printf("ERROR write_activemap rename .old to .1 failed err='%v'", err)
			return false
		}
	}

	if err := os.Rename(local_active_file, filename_bak); err != nil {
		log.Printf("ERROR write_activemap rename BAK failed err='%v'", err)
		return false
	}

	if err := os.Rename(filename_tmp, local_active_file); err != nil {
		log.Printf("ERROR write_activemap move failed .tmp to='%s' err='%v'", local_active_file, err)
		return false
	} else {
		// ALL OK
		log.Printf("INFO local-mode.active wrote=%d lines=%d", wrote, len(list))
		return true
	}
	return false
} // end func write_activemap

func (c *ActiveMap) LoadActiveFile(cfg *config.CFG) {
	if cfg.Settings.ActiveDir == "" {
		log.Printf("ERROR LoadActiveFile cfg.Settings.ActiveDir not set")
		return
	}
	local_active_file := cfg.Settings.ActiveDir + "/local-mode.active"
	file_list, err := ioutil.ReadFile(local_active_file)
	if err != nil {
		log.Printf("ERROR LoadActiveFile failed local_active_file=%s err=%v", local_active_file, err)
		return
	}
	log.Printf("LoadActiveFile read %d bytes", len(file_list))

	data := strings.Split(string(file_list), "\n")
	loaded, bad, badg := 0, 0, []string{}
	for i, line := range data {
		if len(line) <= 0 {
			continue
		}
		values := strings.Split(line, " ")
		if len(values) != 4 {
			log.Printf("ERROR in LoadActiveFile: i=%d file='%s'", i, local_active_file)
			continue
		}

		var ad ActiveData
		group := values[0]
		if !overview.IsValidGroupName(group) {
			//log.Printf("WARN LoadActiveFile !IsValidGroupName ignored group='%s'", group)
			bad++
			badg = append(badg, group)
			continue
		}
		ad.Group = group
		high := utils.Str2uint64(values[1])
		low := utils.Str2uint64(values[2])
		switch low {
		case 0:
			low = 1 // should be at least 1 or thunderbird denies reading a group where low is 0
		case 1:
			ad.Num = high
		default:
			ad.Num = high - low // TODO FIXME does not count or even know about deleted articles
		}
		ad.Hi = high
		ad.Lo = low
		ad.Status = values[3]
		ad.Update = utils.UnixTimeSec()
		ad.Hash = utils.Hash256(group)

		if c.SetActiveData(group, ad) {
			loaded++
		}
	}
	//for _, group := range badg {
	//	fmt.Println("badgroup="+group)
	//}
	log.Printf("LoadActiveFile Groups=%d Bad=%d", loaded, bad)
} // end func readListFile
