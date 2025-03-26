let column_group_data = {
    data_ids: [3, 2],
    group_id: null,
    name: 'SUB PART COMMON',
    group_name: 'SUB PART COMMON ENGLISH',
};

const insert_update_column_group = (column_group_data = {}) => {
    fetchWithLog('api/setting/insert_update_column_group', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(column_group_data),
    })
        .then((response) => response.clone().json())
        .then((json) => {
            displayRegisterMessage('#alert-msg-db', json.flask_message);
        })
        .catch((json) => {
            displayRegisterMessage('#alert-msg-db', json.flask_message);
        });
};

const delete_column_group = (group_id, data_group_id) => {
    fetchWithLog('api/setting/delete_column_group', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            group_id: group_id,
            data_group_id: data_group_id,
        }),
    })
        .then((response) => response.clone().json())
        .then((json) => {
            displayRegisterMessage('#alert-msg-db', json.flask_message);
        })
        .catch((json) => {
            displayRegisterMessage('#alert-msg-db', json.flask_message);
        });
};
