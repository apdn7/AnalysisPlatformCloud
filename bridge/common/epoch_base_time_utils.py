import datetime

NUMBER_OF_DIGIT_OF_TIME_BASE_RANGE = 10
NUMBER_OF_DIGIT_OF_AUTONUMBER = 8

epoch_base_time = datetime.datetime(2020, 1, 1)


def generate_server_id(server_type) -> int:
    """
    How to Id is generated:
        [signed int64]
        8   |888 888 888 8            | 88 888 888
       Type |milliseconds from 2020   | Auto increment
    """
    if not server_type:
        raise Exception('server_type is None')
    delta = datetime.datetime.now() - epoch_base_time
    server_type_id_range = 10 ** (NUMBER_OF_DIGIT_OF_TIME_BASE_RANGE + NUMBER_OF_DIGIT_OF_AUTONUMBER)
    base_time_id_range = 10**NUMBER_OF_DIGIT_OF_AUTONUMBER
    return server_type.value * server_type_id_range + int((round(delta.total_seconds(), 1) * 10)) * base_time_id_range
