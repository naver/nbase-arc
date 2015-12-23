import testarcci
import constant

class TestARCCI32(testarcci.TestARCCI):
    arch = 32
    arcci_log = 'bin/log/arcci_log32'
    so_path = constant.ARCCI32_SO_PATH
