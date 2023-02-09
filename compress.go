package flatfs
//源数据块的解压缩文件
import (
	//"context"
	//"encoding/json"
	//"errors"
	"fmt"
	//"math"
	//"math/rand"
	"os"
	//"path/filepath"
	"strings"
	"sync"
	"github.com/ipfs/go-datastore"
	//"sync/atomic"
	//"syscall"
	"time"
	//cid "github.com/ipfs/go-cid"
	//dshelp "github.com/ipfs/go-ipfs-ds-help"

	"archive/zip"
	"compress/zlib"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"

	"bytes"
	"io"
	"io/ioutil"
)



var maps sync.RWMutex
var mapLit = make(map[string]int, 1000)
//var myTimer = time.Now().Unix() // 启动定时器
var ticker = time.NewTicker(60 * time.Second) //计时器
var ticker1 = time.NewTicker(30 * time.Minute) //计时器

var hclist = make(map[string][]byte)

func hc(key string)[]byte  {
	maps.Lock()
	data:=hclist[key]
	maps.Unlock()
	return data
}
func put_hc(key string,data []byte)  {
	maps.Lock()
	hclist[key]=data
	maps.Unlock()

}
func updata_hc()  {
	println("缓冲大小",len(hclist))
	hclist=make(map[string][]byte)
	println("缓冲大小",len(hclist))
}
//lz4解压缩
func Lz4_compress(val []byte) (value []byte) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	writer.Write(val)
	writer.Close()

	return buf.Bytes()
}
func Lz4_decompress(data []byte) (value []byte ){
	//---------------------------解压
	b:= bytes.NewReader(data)
	//var out bytes.Buffer
	r:= lz4.NewReader(b)
	//io.Copy(&out, r)
	val, err := ioutil.ReadAll(r)
	if  err != nil {
		println("解压错误",err)
		return data
	}

	return val
}
//snappy解压缩
func Snappy_compress(val []byte) (value []byte) {

	//---------------压缩

	var buf bytes.Buffer
	writer := snappy.NewBufferedWriter(&buf)
	writer.Write(val)
	writer.Close()

	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------

	return buf.Bytes()
}
func Snappy_decompress(data []byte) (value []byte ){
	//---------------------------解压
	b:= bytes.NewReader(data)
	//var out bytes.Buffer
	r:=snappy.NewReader(b)
	val, err := ioutil.ReadAll(r)
	if  err != nil {
		println("解压错误",err)
		return data
	}
	//io.Copy(&out,val)
	return val
}
//zip解压缩
func Zip_compress(val []byte) (value []byte) {

	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------
	buf := new(bytes.Buffer)
	w := zip.NewWriter(buf)
	wr, _ := w.CreateHeader(&zip.FileHeader{
		Name:   fmt.Sprintf("block"),
		Method: zip.Deflate, // avoid Issue 6136 and Issue 6138
	})
	wr.Write(val)
	if err := w.Close(); err != nil {
		fmt.Println(err)
	}
	//fmt.Println(len(val))
	//fmt.Println(len(buf.Bytes()))
	return buf.Bytes()
}
func Zip_decompress(data []byte) (value []byte ){
	//---------------------------解压
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		fmt.Println(err)
		return data
	}
	r, _ := zr.File[0].Open()
	defer r.Close()
	//var out bytes.Buffer
	//_, err = io.Copy(&out, r)
	val, err := ioutil.ReadAll(r)
	if  err != nil {
		println("解压错误",err)
		return data
	}
	return val
}
//zlib解压缩
func Zlib_compress(val []byte) (value []byte) {


	//---------------压缩
	var buf bytes.Buffer
	compressor := zlib.NewWriter(&buf)
	compressor.Write(val)
	compressor.Close()
	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------

	return buf.Bytes()
}
func Zlib_decompress(data []byte) (value []byte ){
	//---------------------------解压
	b:= bytes.NewReader(data)
	var out bytes.Buffer
	r,err:= zlib.NewReader(b)
	if  err != nil {
		println("解压错误",err)
		return data
	}
	io.Copy(&out, r)
	return out.Bytes()

}
//Zstd解压缩
func Zstd_compress(val []byte) (value []byte) {


	var buf bytes.Buffer
	writer,_ := zstd.NewWriter(&buf)
	writer.Write(val)
	writer.Close()

	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------

	return buf.Bytes()
}
func Zstd_decompress(data []byte) (value []byte ){
	//---------------------------解压
	b:= bytes.NewReader(data)
	//var out bytes.Buffer
	r,err:= zstd.NewReader(b)
	val, err := ioutil.ReadAll(r)
	if  err != nil {
		println("解压错误",err)
		return data
	}
	//io.Copy(&out, r)
	return val
}

func Pr() {

	//fmt.Println("-------------------------------")
	//for i,n:= range mapLit{
	//	fmt.Println(i,n)
	//}
	//fmt.Println("-------------------------------")

	mapLit = make(map[string]int, 1000)

}

func Jl(key string) {
	//------------------------------------------------------------
	//s:= dshelp.MultihashToDsKey(k.Hash()).String()
	s:=key
	s = strings.Replace(s, "/", "", -1)
	if mapLit[s]<99{
		maps.Lock()
		mapLit[s]+=1
		maps.Unlock()
	}

	//var endtime =time.Now().Unix()
	//stime:=endtime-myTimer
	//// do sth repeatly
	//if stime>=30{
	//	fmt.Println("-------------------------------")
	//	for i,n:= range mapLit{
	//		fmt.Println(i,n)
	//	}
	//	fmt.Println("-------------------------------")
	//	mapLit = make(map[string]int, 1000)
	//	myTimer =time.Now().Unix()
	//
	//}
}
func Deljl(key string)  {
	//---------------------------------------------------------------------
	maps.Lock()
	s:= key
	s = strings.Replace(s, "/", "", -1)
	delete(mapLit,s)
	maps.Unlock()
	//var endtime =time.Now().Unix()
	//stime:=endtime-myTimer
	//// do sth repeatly
	//if stime>=30{
	//	fmt.Println("-------------------------------")
	//	for i := range mapLit{
	//		fmt.Println(i)
	//	}
	//	fmt.Println("-------------------------------")
	//	mapLit = make(map[string]int, 1000)
	//	myTimer =time.Now().Unix()
	//
	//}
	//------------------------------------------------------------------------
}
func getmap(key string)int{
	return mapLit[key]
}
func (fs *Datastore) dohotPut(key datastore.Key, val []byte) error {

	dir, path := fs.encode(key)
	if err := fs.makeDir(dir); err != nil {
		return err
	}

	tmp, err := fs.tempFile()
	if err != nil {
		return err
	}
	closed := false
	removed := false
	defer func() {
		if !closed {
			// silence errcheck
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}
	}()

	if _, err := tmp.Write(val); err != nil {
		return err
	}
	if fs.sync {
		if err := syncFile(tmp); err != nil {
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true

	err = fs.renameAndUpdateDiskUsage(tmp.Name(), path)
	if err != nil {
		return err
	}
	removed = true

	if fs.sync {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return nil
}


func (fs *Datastore) Get_writer(dir string,path string) ( err error) {

	data, err := readFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return datastore.ErrNotFound
		}
		// no specific error to return, so just pass it through
		return  err
	}
	if err := fs.makeDir(dir); err != nil {
		return err
	}

	tmp, err := fs.tempFile()
	if err != nil {
		return err
	}

	//压缩
	fmt.Printf("get_writer触发\n")
	//Jl(key.String())
	va:=Lz4_compress(data)
	if _, err := tmp.Write(va); err != nil {
		return err
	}
	if fs.sync {
		if err := syncFile(tmp); err != nil {
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	err = fs.renameAndUpdateDiskUsage(tmp.Name(), path)
	if err != nil {
		return err
	}
	if fs.sync {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	defer tmp.Close()
	defer os.Remove(tmp.Name())


	return nil
}