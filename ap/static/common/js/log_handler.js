const LIMIT_ROW_IN_LOG = 10000; // Only get latest among of log

const onOffDebugLog = (is_set_on = true) => {
    const $sideBarDebug = $('div.sidebar-footer.sidebar-debug');
    if (is_set_on) {
        $sideBarDebug.removeClass('d-none');
        localStorage.setItem('DEBUG', 'true');
        window.isDebugMode = true;
    } else {
        $sideBarDebug.addClass('d-none');
        localStorage.setItem('DEBUG', 'false');
        window.isDebugMode = false;
        window.log = [];
    }
};

const injectExportDebugLogEvent = () => {
    const $btnExportDebugLog = $('#btnExportDebugLog');
    const exportDebugLogHandler = () => {
        const link = document.createElement('a');
        link.href = 'data:text/plain;charset=UTF-8,' + window.log.join('\n');
        link.download = `BS_frontend_${moment().format('YYYYMMDDHHmmsssZ')}.log`;
        link.click();
    };

    $btnExportDebugLog.off().click(() => {
        exportDebugLogHandler();
    });
};

const consoleLogDebug = (msg = '') => {
    // If you want to show log, you must set DEBUG=true on localStorage first !!!
    if (!window.isDebugMode) return;
    const message = `${moment().format('YYYY-MM-DD HH:mm:sssZZ')} ${msg}`;
    window.log.push(message);
    if (window.log.length > window.LIMIT_ROW_IN_LOG) {
        window.log = window.log.slice(
            window.log.length - window.LIMIT_ROW_IN_LOG,
        );
    }

    console.debug(message);
};

const consoleLogDebugForAjax = (option = {}, response = {}, error = {}) => {
    // If you want to show log, you must set DEBUG=true on localStorage first !!!
    if (!window.isDebugMode) return;

    const optionStr = `\n\nOption:\n${JSON.stringify(option)}`;
    const responseStr =
        Object.keys(response ?? {}).length > 0
            ? `\n\nResponse:\n${JSON.stringify(response)}`
            : '';
    const errorStr =
        Object.keys(error ?? {}).length > 0
            ? `\n\nError:\n${JSON.stringify(error)}`
            : '';
    consoleLogDebug(`[AJAX]${optionStr}${responseStr}${errorStr}\n\n`);
};

const consoleLogDebugForFetch = (
    url = '',
    option = {},
    response = {},
    error = {},
) => {
    // If you want to show log, you must set DEBUG=true on localStorage first !!!
    if (!window.isDebugMode) return;

    const optionStr = `\n\nOption:\n${JSON.stringify(option)}`;
    const responseStr =
        Object.keys(response ?? {}).length > 0
            ? `\n\nResponse:\n${JSON.stringify(response)}`
            : '';
    const errorStr =
        Object.keys(error ?? {}).length > 0
            ? `\n\nError:\n${JSON.stringify(error)}`
            : '';
    consoleLogDebug(
        `[FETCH] Url=${url}${optionStr}${responseStr}${errorStr}\n\n`,
    );
};

const ajaxWithLog = (option = {}) => {
    const _injectLogToFunc = (functionName) => {
        if (functionName in option) {
            const exist_func = option[functionName];
            if (option[functionName].constructor.name === 'AsyncFunction') {
                if (functionName === 'success') {
                    option[functionName] = async (data, textStatus, jqXHR) => {
                        consoleLogDebugForAjax(option, data);
                        await exist_func(data, textStatus, jqXHR);
                    };
                } else {
                    option[functionName] = async (
                        jqXHR,
                        textStatus,
                        errorThrown,
                    ) => {
                        consoleLogDebugForAjax(
                            option,
                            undefined,
                            jQuery.extend({}, jqXHR),
                        );
                        await exist_func(jqXHR, textStatus, errorThrown);
                    };
                }
            } else {
                if (functionName === 'success') {
                    option[functionName] = (data, textStatus, jqXHR) => {
                        consoleLogDebugForAjax(option, data);
                        exist_func(data, textStatus, jqXHR);
                    };
                } else {
                    option[functionName] = (jqXHR, textStatus, errorThrown) => {
                        consoleLogDebugForAjax(
                            option,
                            undefined,
                            jQuery.extend({}, jqXHR),
                        );
                        exist_func(jqXHR, textStatus, errorThrown);
                    };
                }
            }
        } else {
            if (functionName === 'success') {
                option[functionName] = (data) => {
                    consoleLogDebugForAjax(option, data);
                };
            } else {
                option[functionName] = (jqXHR) => {
                    consoleLogDebugForAjax(
                        option,
                        undefined,
                        jQuery.extend({}, jqXHR),
                    );
                };
            }
        }
    };

    _injectLogToFunc('success');
    _injectLogToFunc('error');

    return $.ajax(option);
};

const fetchWithLog = (url = '', option = {}) => {
    return fetch(url, option)
        .then((response) => {
            if (!response.ok) {
                consoleLogDebugForFetch(
                    url,
                    option,
                    undefined,
                    jQuery.extend({}, response),
                );
            } else {
                consoleLogDebugForFetch(url, option, response);
            }

            return response;
        })
        .catch((error) => {
            consoleLogDebugForFetch(
                url,
                option,
                undefined,
                jQuery.extend({}, error),
            );
        });
};

$(() => {
    if (localStorage.getItem('DEBUG')) {
        window.isDebugMode =
            localStorage.getItem('DEBUG').trim().toLowerCase() === 'true';
        if (window.isDebugMode) {
            $('div.sidebar-footer.sidebar-debug').removeClass('d-none');
        }
    } else {
        window.isDebugMode = false;
    }
    window.log = window.log ?? [];

    if (localStorage.getItem('LIMIT_ROW_IN_LOG')) {
        window.LIMIT_ROW_IN_LOG = parseInt(
            localStorage.getItem('LIMIT_ROW_IN_LOG'),
        );
    } else {
        window.LIMIT_ROW_IN_LOG = LIMIT_ROW_IN_LOG;
        localStorage.setItem('LIMIT_ROW_IN_LOG', LIMIT_ROW_IN_LOG.toString());
    }

    injectExportDebugLogEvent();
});
