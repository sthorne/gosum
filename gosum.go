// Copyright 2014 Sean Thorne.  All rights reserved.
// Use of this code is governed by the license found in LICENSE
package main

import (

    "io"
    "os"
    "fmt"
    "flag"
    "sort"
    "bufio"
    "reflect"
    "strconv"
    "strings"
    "crypto/md5"
    "encoding/hex"

)

// type to hold the unique values from each file
type File struct {
    Filename        string          `json:"Filename"        xml:"Filename"`
    Value           float64         `json:"Value"           xml:"Value"`
}

func NewFile(f string, v float64) *File {
    return &File{Filename: f, Value: v}
}

// type to hold the data read from the files
type Row struct {
    Columns         []string        `json:"Columns"         xml:"Columns"`
    Values          []*File         `json:"Values"          xml:"Values"`
    Hash            string          `json:"Hash,omit"       xml:"Hash,omit"`
    Total           float64         `json:"Value"           xml:"Value"`
}

// add to the total value
func (r *Row) Add(n float64, f string) {
    r.Total += n
    r.Values = append(r.Values, NewFile(f, n))
}

// create a new Row pointer
func NewRow(c []string, h string) *Row {
    return &Row{Columns: c, Hash: h, Total: 0, Values: []*File{}}
}

// Type for sorting the resulting dataset using the go sort package
// Uses the standard Len, Less and Swap methods
type Summary []*Row

func (s Summary) Len() int {
    return len(s)
}

func (s Summary) Less(i, j int) bool {
    return s[i].Columns[0] < s[j].Columns[0]
}

func (s Summary) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}

// our global vars
var (
    column      int
    seperator   string
    files       []string
    fileCount   int
    output      string
    summary     map[string]*Row
)

// setup our flags
func init() {
    flag.IntVar(&column, "column", 1, "column to summarize")
    flag.StringVar(&seperator, "seperator", ",", "the text used to seperate the columns")
    flag.StringVar(&output, "output", "", "the file to output the results into")

    summary = make(map[string]*Row)
}

// run baby, run
func main() {

    flag.Parse()
    fileCount := flag.NArg()

    if fileCount > 0 {
        readfile(fileCount)
    } else {
        readstdin()
    }

    write()
}

// read from stdin
func readstdin() {
    readinput(os.Stdin, "stdin")
}

// read from the list of files provied on the command line
func readfile(fileCount int) {

    for i := 0; i < fileCount; i++ {

        f := flag.Arg(i)

        if !fileexists(f) {
            fmt.Println(f, "Could not be found... skipping")
            continue
        }

        r, e := os.Open(f)

        if e != nil {
            fmt.Println("Could not open file " + f, "... skipping")
            continue
        }

        readinput(r, f)
    }
}

// read in the data stream
func readinput(r io.Reader, f string) {

    s := bufio.NewScanner(r)

    ln := 0
    for s.Scan() {
        ln++
        l := s.Text()
        c := strings.Split(strings.TrimSpace(l), seperator)

        if len(c) == 0 {
            readerr("No columns parsed from line", f, ln, nil)
            continue
        }

        if column > len(c) {
            readerr("No enough columns to meet requested column number", f, ln, nil)
            continue
        }

        v, e := strconv.ParseFloat(c[column - 1], 64)

        if e != nil {
            readerr("Failed to parse line", f, ln, e)
        }

        // remove the value column since we don't to output that in the result
        columns := []string{}
        for k, v := range c {
            if k == (column - 1) {
                continue
            }
            columns = append(columns, v)
        }

        k := hash(strings.Join(columns, ""))

        if _, ok := summary[k]; !ok {
            summary[k] = NewRow(columns, k)
        }

        summary[k].Add(v, f)
    }

    if e := s.Err(); e != nil {
        panic(e)
    }

}

// convenience function if there's a read error since we 
// have a couple places this could be used
func readerr(m string, f string, l int, e error) {

    ErrStr := ""
    if e != nil {
        ErrStr = e.Error()
    }

    fmt.Println(m, l, "in file", f, "with error", ErrStr)
}

// simple output
func write() {

    w := os.Stdout
    
    if output != "" {
        w = os.Open(output)
    }

    s := Summary{}

    for _, r := range summary {
        s = append(s, r)
    }

    sort.Sort(s)

    for _, r := range s {
        fmt.Fprint(w, r.Total)

        if len(r.Values) > 0 {
            for _, v := range r.Columns {
                fmt.Fprint(w, seperator, v)
            }
        }

        fmt.Fprint(w, "\n")
    }
}

// utility function for seeing if a file exists
func fileexists(f string) bool {
    _, e := os.Stat(f)

    if os.IsNotExist(e) {
        return false
    }

    return true
}

// create an int for a map key
func hash(i interface{}) string {

    var s []byte

    switch i.(type) {
    case []byte:
        s = reflect.ValueOf(i).Bytes()

    case string:
        s = []byte(reflect.ValueOf(i).String())

    default:
        panic("Cannot use interface type given in hash")
    }

    h := md5.New()

    h.Write(s)

    b := h.Sum(nil)

    return hex.EncodeToString(b)
}