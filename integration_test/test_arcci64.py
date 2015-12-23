import testarcci
import constant

class TestARCCI64(testarcci.TestARCCI):
    arch = 64
    arcci_log = 'bin/log/arcci_log64'
    so_path = constant.ARCCI_SO_PATH
