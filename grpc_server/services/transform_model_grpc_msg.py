from google.protobuf.json_format import MessageToJson


def model_to_msg(source, msg_cls):
    """
    set model value to grpc message
    :param source: model instance
    :param msg_cls: grpc message class object
    :return:
    """
    msg = msg_cls()
    for col in source.__table__.columns:
        col_name = col.key

        # check valid column
        if not hasattr(msg, col_name):
            continue

        # get value
        val = getattr(source, col_name)
        # set value to msg column
        if val is not None:
            setattr(msg, col_name, val)

    return msg


def msg_to_json(msg):
    """
    convert msg to json
    :param msg:
    :return:
    """
    json_msg = MessageToJson(msg)
    return json_msg
