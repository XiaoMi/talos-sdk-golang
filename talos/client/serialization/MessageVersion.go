/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com  
*/

package serialization

type MessageVersion int

const (
    V1 MessageVersion = iota + 1 //1
    V2
    V3
)

func (v MessageVersion) String() string {
    switch v {
    case V1:
        return "V1"
    case V2:
        return "V2"
    case V3:
        return "V3"
    default:
        return "UNKNOWN"
    }
}
