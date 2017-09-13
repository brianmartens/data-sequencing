package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	//"sync"
	"time"
)

var skillMap = map[string]string{
	"NMAAHC":               "Main Menu",
	"NMAAHC 10":            "Advance Passes",
	"NMAAHC 11":            "Same Day/Walk Up Passes",
	"NMAAHC 12":            "Cancel/Exchange Passes",
	"NMAAHC TICKET":        "Obtain Passes Over Phone",
	"NMAAHC XFER VALID":    "Valid Transfer",
	"NMAAHC SHOP":          "Museum Shop",
	"NMAAHC CAFE":          "Sweet Home Cafe",
	"NMAAHC 14":            "New Group Reservation",
	"NMAAHC BusyNoAnswer":  "Network Error/Busy",
	"NMAAHC NetInvalidNum": "Network Error/Busy",
	"NMAAHC 18":            "Cancel Group Reservation",
	"NMAAHC MEM RENEW":     "Donations, Gifts, New/Renew Membership",
	"NMAAHC MEM QUESTION":  "Questions About Membership",
	"NMAAHC 17":            "Donate Object or Collection",
	"NMAAHC 2":             "Timed Entry Passes",
	"NMAAHC 3":             "Group Reservations",
	"NMAAHC 4":             "Membership & Donations",
	"NMAAHC 5":             "Location & Hours",
	"NMAAHC 6":             "Transportation & Parking",
	"NMAAHC 7":             "Tours",
	"NMAAHC 8":             "Accessibility",
	"NMAAHC 9":             "Sweet Home Cafe and the Museum Shop",
}

var TICKET string = "Wednesday"

var iID, iMID, iTIME, iCAMP, iCONTACT, iSKILL, iDATE int

type entry struct {
	assigned int
	values   []string
}

type sequence struct {
	seqID   int
	entries map[int]entry
}

type sequenceList struct {
	index     int
	sequences map[int]sequence
}

func (seqList *sequenceList) addSequence(seq *sequence) {
	seqList.sequences[seqList.index] = *seq
	seq.seqID = seqList.index
	seqList.index++

}

func (seq *sequence) addEntry(id int, ent *entry) {
	ent.assigned = seq.seqID
	seq.entries[id] = *ent

}

func (seq *sequence) merge(seq2 *sequence) {
	for id, ent := range seq2.entries {
		if _, ok := seq.entries[id]; !ok {
			seq.addEntry(id, &ent)

		}

	}

}

func (ent *entry) forTrace(entries map[int]*entry) (*sequence, error) {
	// if it is the beginning of a sequence, then add start the sequence with ent and delete it from entries
	if id, err := strconv.Atoi(ent.values[iID]); err != nil {
		return &sequence{}, err

	} else {
		seq := &sequence{}
		seq.entries = make(map[int]entry)
		seq.addEntry(id, ent)
		defer delete(entries, id)
		for _, spawn := range entries {
			if spawn.assigned == -1 {
				if bytes.Equal([]byte(ent.values[iID]), []byte(spawn.values[iMID])) {
					if seq2, err := spawn.forTrace(entries); err != nil {
						return seq, err

					} else {
						seq.merge(seq2)

					}

				}

			}

		}
		return seq, nil

	}

}

func (seq *sequence) xferReason() string {
	for _, ent := range seq.entries {
		if ent.values[iSKILL] == "NMAAHC TICKET" {
			return "TICKETING"

		} else if ent.values[iSKILL] == "NMAAHC MEM RENEW" ||
			ent.values[iSKILL] == "NMAAHC MEM QUESTION" {
			return "MEMBERSHIP"

		}

	}
	return ""

}

func isTDay(start_date time.Time) bool {
	start_year, start_month, _ := start_date.Date()
	month_start := time.Date(start_year, start_month, 1, 1, 1, 1, 1, time.UTC)
	for {
		if month_start.Weekday().String() == TICKET {
			break

		} else {
			month_start = month_start.Add(time.Hour * 24)

		}

	}
	return start_date.Day() == month_start.Day()

}

func main() {
	sFile := "Data.csv"
	fileStr, err := ioutil.ReadFile(sFile)
	if err != nil {
		fmt.Println(err)

	}
	str := string(fileStr)
	entries := make(map[int]*entry)
	seqList := sequenceList{0, make(map[int]sequence)}
	r := csv.NewReader(strings.NewReader(str))
	headers, err := r.Read()
	headers = append(headers, "S_ID", "TIME_30", "TIME_15", "CALL_SUM", "AREA", "WEEKDAY", "T_DAY")
	if err != nil {
		fmt.Println(err)
	}
	for i, sHeader := range headers {
		if sHeader == "contact_id" {
			iID = i

		} else if sHeader == "master_contact_id" {
			iMID = i

		} else if sHeader == "start_time" {
			iTIME = i

		} else if sHeader == "campaign_name" {
			iCAMP = i

		} else if sHeader == "skill_name" {
			iSKILL = i

		} else if sHeader == "contact_name" {
			iCONTACT = i

		} else if sHeader == "start_date" {
			iDATE = i

		}

	}
	for {
		record, err := r.Read()
		if err != nil {
			break

		}
		// make each entry have an initial assigned = -1
		if record[iCAMP] == "NMAAHC" {
			if id, err := strconv.Atoi(record[iID]); err != nil {
				fmt.Println(err)

			} else {
				entries[id] = &entry{-1, record}

			}

		}

	}
	//var waitGroup sync.WaitGroup
	for _, ent := range entries {
		if ent.assigned == -1 {
			if bytes.Equal([]byte(ent.values[iID]), []byte(ent.values[iMID])) {
				if seq, err := ent.forTrace(entries); err != nil {
					fmt.Println(err)

				} else {
					seqList.addSequence(seq)

				}

			}
		}

	}
	//waitGroup.Wait()
	file, err := os.Create("callSequence.csv")
	w := csv.NewWriter(file)
	if err := w.Write(headers); err != nil {
		log.Fatalln(err)

	}
	for sID, seq := range seqList.sequences {
		var index int = 1
		for _, ent := range seq.entries {
			timeStamp, err := time.Parse("15:04:05", ent.values[iTIME])
			if err != nil {
				fmt.Println(err)

			}
			var intervals [2]string
			var iMin [2]int = [2]int{int(math.Floor(float64(timeStamp.Minute())/30.0) * 30),
				int(math.Floor(float64(timeStamp.Minute())/15.0) * 15)}
			for i := range intervals {
				if timeStamp.Hour() < 10 {
					intervals[i] += "0" + strconv.Itoa(timeStamp.Hour())

				} else {
					intervals[i] += strconv.Itoa(timeStamp.Hour())

				}
				if iMin[i] < 10 {
					intervals[i] += ":0" + strconv.Itoa(iMin[i])

				} else {
					intervals[i] += ":" + strconv.Itoa(iMin[i])

				}
				intervals[i] += ":00"

			}
			if ent.values[iCONTACT] == "Outbound" {
				ent.values[iCONTACT] = func(s *sequence) string {
					for _, e := range s.entries {
						if e.values[iCONTACT] != "Outbound" {
							return e.values[iCONTACT]

						}

					}
					return ""

				}(&seq)

			}
			startDate, err := time.Parse("01/02/2006", ent.values[iDATE])
			if err != nil {
				fmt.Println(err)

			}
			var AREA string
			if ent.values[iSKILL] == "NMAAHC XFER VALID" ||
				ent.values[iSKILL] == "NMAAHC BusyNoAnswer" ||
				ent.values[iSKILL] == "NMAAHC NetInvalidNum" {
				AREA = seq.xferReason() + " " + skillMap[ent.values[iSKILL]]

			} else {
				AREA = skillMap[ent.values[iSKILL]]

			}
			ent.values = append(ent.values, // Array being appended to
				strconv.Itoa(sID), intervals[0], intervals[1], strconv.Itoa(index), // Sequence ID, TIME_30, TIME_15
				AREA, startDate.Weekday().String(), strconv.FormatBool(isTDay(startDate))) // Call sum, AREA, WEEKDAY, T_DAY
			if err := w.Write(ent.values); err != nil {
				log.Fatalln(err)

			}
			index = 0

		}

	}
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatal(err)

	}

}
