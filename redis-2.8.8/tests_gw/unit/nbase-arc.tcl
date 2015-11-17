# For NBASE-ARC

start_server {tags {"arc_basic"}} {
    test {Multikey commands with null character} {
        set fd [r channel]
        set null_char [format %c 0]
        set res {}

        puts -nonewline $fd "*3\r\n\$4\r\nMSET\r\n\$7\r\nkey${null_char}key\r\n\$7\r\nval${null_char}val\r\n"
        flush $fd
        append res [::redis::redis_read_reply $fd]

        puts -nonewline $fd "*2\r\n\$4\r\nMGET\r\n\$7\r\nkey${null_char}key\r\n"
        flush $fd
        append res [string match "val${null_char}val" [::redis::redis_read_reply $fd]]
    } {OK1}

    test {Commands pipelining - NBASE ARC} {
        set fd [r channel]
        puts -nonewline $fd "SET 1 1\r\nSET 2 2\r\nSET 3 3\r\nSET 4 4\r\nGET 1\r\nGET 2\r\nGET 3\r\nGET 4\r\nGET 1\r\n"
        flush $fd
        set res {}
        append res [string match OK* [::redis::redis_read_reply $fd]]
        append res [string match OK* [::redis::redis_read_reply $fd]]
        append res [string match OK* [::redis::redis_read_reply $fd]]
        append res [string match OK* [::redis::redis_read_reply $fd]]
        append res [::redis::redis_read_reply $fd]
        append res [::redis::redis_read_reply $fd]
        append res [::redis::redis_read_reply $fd]
        append res [::redis::redis_read_reply $fd]
        append res [::redis::redis_read_reply $fd]
        format $res
    } {111112341}

    tags {protocol} {
        test {PIPELINING stresser (test ordering of pipelined execution of gateway)} {
            set fd2 [socket $::host $::gwport]
            fconfigure $fd2 -encoding binary -translation binary

            for {set i 0} {$i < 100000} {incr i} {
                set q {}
                set val "0000${i}0000"
                append q "SET key $val\r\n"
                puts -nonewline $fd2 $q
                set q {}
                append q "GET key\r\n"
                puts -nonewline $fd2 $q
            }
            flush $fd2

            for {set i 0} {$i < 100000} {incr i} {
                gets $fd2 line
                gets $fd2 count
                set count [string range $count 1 end]
                set val [read $fd2 $count]
                read $fd2 2
                assert_equal "0000${i}0000" $val
                
            }
            close $fd2
            set _ 1
        } {1}
    }
}
