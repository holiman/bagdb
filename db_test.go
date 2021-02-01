// bagdb: Simple datastorage
// Copyright 2021 bagdb authors
// SPDX-License-Identifier: BSD-3-Clause

package bagdb


import (
	"os"
	"testing"
)

func TestGrowFile(t *testing.T){
	f, err := os.Create("tempfile")
	if err != nil{
		t.Fatal(err)
	}
	//defer os.Remove("tempfile")
	defer f.Close()
	f.Seek(55,0)
	//f.WriteAt()
	f.Write([]byte("test"))
}

func TestGrowFile2(t *testing.T){
	f, err := os.Create("tempfile2")
	if err != nil{
		t.Fatal(err)
	}
	//defer os.Remove("tempfile")
	defer f.Close()
	//f.Seek(55,0)
	f.WriteAt([]byte("test"), 55)
	//f.Write([]byte("test"))
}
