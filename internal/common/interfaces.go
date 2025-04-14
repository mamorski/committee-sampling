package common

type FilterF func(string, []byte, []byte, *AuxKey) bool

type FilterTagF func(string, []byte, []byte, *AuxTag) bool

type GradeFunc func(string, []byte, []byte, *AuxKey, float64) int
