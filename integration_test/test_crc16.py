import unittest
import testbase
import util
import gateway_mgmt
import config
import default_cluster


class TestCRC16( unittest.TestCase ):
    cluster = config.clusters[0]

    @classmethod
    def setUpClass( cls ):
        ret = default_cluster.initialize_starting_up_smr_before_redis( cls.cluster )
        if ret  is not 0:
            default_cluster.finalize( cls.cluster )

    @classmethod
    def tearDownClass( cls ):
        default_cluster.finalize( cls.cluster )

    def setUp( self ):
        util.set_process_logfile_prefix( 'TestCRC16_%s' % self._testMethodName )
        return 0

    def tearDown( self ):
        return 0

    def test_single_thread_input( self ):
        util.print_frame()
        self.cluster = config.clusters[0]
        result = {}

        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( ip )
        self.assertEquals( 0, gw.connect( ip, port ) )

        max = 5
        for idx in range( max ):
            cmd = 'set key%d 0\r\n' % (idx)
            gw.write( cmd )
            result[idx] = gw.read_until( '\r\n' )

        data_max = 65535
        for idx in range( max ):
            for cnt in range( 0, data_max ):
                gw.write( 'crc16 key%d %d\r\n' % (idx, cnt) )
                result[idx] = gw.read_until( '\r\n' )

        for idx in range( max - 1 ):
            self.assertEquals( result[idx], result[idx + 1] )
