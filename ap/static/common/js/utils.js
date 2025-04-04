/* eslint-disable no-useless-escape */

let xhr = null;
const SQL_LIMIT = 50000;
let dataSetID = null;
let $closeAllToast = null;
let showElapsedTime = null;
const LOADING_TIMEOUT_FOR_COLUMN_ATTR = 10000; // 10 seconds

const zeroFillPatternRaw = /(^{:0[<>]?([1-9]\d*d?)}$)|(^{:([1-9][<>]\d+d?)}$)/;
const zeroFillPattern = /(^0[<>]?[1-9]\d*d?$)|(^[1-9][<>]\d+d?$)/;
const percentagePattern = /^(.\d+)?%$/;
const RLP_DUMMY_ID = 1000001;
const EMD_DIFF_ID = 1000000;

const localeConst = {
    JP: 'ja',
};

const DATETIME_FORMAT = 'YYYY-MM-DD HH:mm:ss';
const DATETIME_FORMAT_PLACE_HOLDER = '%Y-%m-%d %H:%M:%S.%f';
const THOUSAND_SEP_PATTERN = /^[0-9]{1,3}(,[0-9]{3})*(\.[0-9]+)?$/;

const COMMON_CONSTANT = {
    NA: 'NA',
    EN_DASH: '–',
    INF: 'Inf',
    MINF: '-Inf',
    DEFAULT_SUB_TITLE: 'DN7QCTools',
    TAB: '&#09;',
    TICKS_ANGLE: 0,
    NG_RATE: 'ng_rates',
    NG_RATE_NAME: 'NG Rate',
    EMD_DRIFT_NAME: 'EMD|Drift',
    EMD_DIFF_NAME: 'EMD|Diff',
};

const MESSAGE_LEVEL = {
    WARN: 'WARN',
    ERROR: 'ERROR',
    INFO: 'INFO',
};

const scaleOptionConst = {
    SETTING: '1',
    COMMON: '2',
    THRESHOLD: '3',
    AUTO: '4',
    FULL_RANGE: '5',
};

const frequencyOptions = {
    COMMON: '1',
    AUTO: '2',
};

// facet level definition
const facetLevels = {
    LV_1: '1',
    LV_2: '2',
    DIV: '3',
    UNSET: '',
};

const EMDType = {
    DRIFT: 'drift',
    DIFF: 'diff',
    BOTH: 'both',
};

const TIME_UNIT = {
    hour: {
        MIN: 0.01,
        MAX: 20000,
        DEFAULT: 24,
    },
    minute: {
        MIN: 0.01,
        MAX: 1000000,
        DEFAULT: 60,
    },
    day: {
        MIN: 0.01,
        MAX: 1000,
        DEFAULT: 1,
    },
    week: {
        MIN: 0.01,
        MAX: 100,
        DEFAULT: 1,
    },
    month: {
        MIN: 0.01,
        MAX: 24,
        DEFAULT: 1,
    },
    year: {
        MIN: 0.01,
        MAX: 2,
        DEFAULT: 1,
    },
};

const CYCLIC_TERM = {
    NAME: 'cyclicTerm',
    INTERVAL: 'cyclicTermInterval',
    WINDOW_LENGTH: 'cyclicTermWindowLength',
    DIV_NUM: 'cyclicTermDivNum',
    DIV_CALENDER: 'divideFormat',
    DIV_OFFSET: 'divideOffset',
    DATA_NUMBER: 'dataNumber',
    RECENT_INTERVAL: 'recentTimeInterval',
    INTERVAL_MIN_MAX: {
        MIN: -720,
        MAX: 720,
        DEFAULT: 0.01,
    },
    WINDOW_LENGTH_MIN_MAX: {
        MIN: 0.01,
        MAX: 20000,
        DEFAULT: 1,
    },
    DIV_NUM_MIN_MAX: {
        MIN: 2,
        MAX: 150,
        DEFAULT: 30,
    },
    DIV_OFFSET_MIN_MAX: {
        MIN: -24,
        MAX: 24,
        DEFAULT: 0,
    },
    TIME_UNIT: TIME_UNIT.hour,
};

const EXPORT_TYPE = {
    TSV: 'TSV',
    CSV: 'CSV',
    TSV_CLIPBOARD: 'TSV-CLIPBOARD',
};

const EXPORT_DOM = {
    DROPDOWN_ID: '#export-dropdown',
    INPUT: 'input[name=export_from]:checked',
};

const EXPORT_DATA_SRC = {
    ALL: 'all',
    PLOT: 'plot',
};

const CONST = {
    RED: 'rgb(255,4,4)',
    BLUE: 'rgb(109,205,250)',
    DARK_BLUE: '#268fff',
    COLOR_INF: 'rgba(255, 255, 255, 1)', // white
    COLOR_NONE: 'rgba(204, 204, 204, 0.5)', // grey80
    COLOR_OUTLIER: 'red',
    COLOR_UNLINKED: '#666666',
    COLOR_NORMAL: '#89b368',
    COLOR_THIN: 'rgb(52, 169, 234)',
    COLOR_ERROR_BAR: 'rgb(66, 104, 125)',
    COLOR_FRAME_BORDER: 'rgb(96,96,96)',
    COLOR_FRAME_BACKGROUND: 'rgb(0, 0, 0, 0)',
    GRID: 'rgba(96,96,96,0.2)',
    TICK: '#dedede',
    CH_SELF: '#ffffff', // cross hair color
    CH_OTHER: '#7f7f7f', // cross hair color
    LIGHT_BLUE: '#65c5f1',
    WHITE: '#ffffff',
    YELLOW: 'yellow',
    BGR_COLOR: '#222222',

    COLORBAR: {
        fontsize: 13,
    },

    // y value type to differentiate normal vs irregular data
    NORMAL: 0,
    NONE: 2,
    OUTLIER: 3,
    NEG_OUTLIER: -3,
    UNLINKED: -4,

    // dataset index of time-series chart
    NORMAL_DATASET: 0,
    IRREGULAR_DATASET: 1,

    // annotation ids
    UCL: 'ucl',
    LCL: 'lcl',
    vUCL: 'vucl',
    vLCL: 'vlcl',
    UPCL: 'upcl',
    LPCL: 'lpcl',
    vUPCL: 'vupcl',
    vLPCL: 'vlpcl',
    VERTICAL: 'v',
    HORIZONTAL: 'h',
    ALL: 'all',

    COMMON: 'COMMON',
    STARTDATE: 'START_DATE',
    STARTTIME: 'START_TIME',
    ENDDATE: 'END_DATE',
    ENDTIME: 'END_TIME',
    CONDT: 'cond_procs',
    FILTER_PARTNO: 'filter-partno',
    FILTER_LINE: 'filter-line-machine-id',
    FILTER_MACH: 'machine_id_multi',
    FILTER_OTHER: 'filter-other',
    NO_FILTER: 'NO_FILTER',
    XOPT_TIME: 'TIME',
    XOPT_INDEX: 'INDEX',
    ARRAY_FORMVAL: 'ARRAY_FORMVAL',
    CATEGORY: 'category',
    CORR: 'correlation',

    NAV: ['', 'null', null, 'nan', 'NA', '-inf', 'inf'],
    BGR_COLOR_ATTR: 'bgr-color',
    BGR_COLOR_KEY: 'background-color',
    DEFAULT_VALUE: 'default-value',
    SMALL_DATA_SIZE: 256,
    INF: 'inf',
    NEG_INF: '-inf',
    NO_LINKED: 'NoLinked',
    LIMIT_SIZE_NAME: 14,
    IS_ADMIN: 'is_admin',
    RESIZE_RANGE_CHART: 0.5,
};
const chmColorPalettes = [
    ['0', '#18324c'],
    ['0.2', '#204465'],
    ['0.4', '#2d5e88'],
    ['0.6', '#3b7aae'],
    ['0.8', '#56b0f4'],
    ['1', '#6dc3fd'],
];

let reselectCallback = null;
const domEles = {
    problematicTbl: '#cols-variance-tbl',
    problematicModal: '#removeErrorColumnMdl',
    problematicPCATbl: '#cols-variance-tbl-multiple-range',
};

const trimLeft = (target) => target.replace(new RegExp(/^[\s]+/), '');

const trimRight = (target) => target.replace(new RegExp(/[\s]+$/), '');

const trimMid = (target) => target.replace(new RegExp(/\s+/), ' ');

const trimBoth = (target) => trimLeft(trimRight(trimMid(target)));

const isEmpty = (val) => {
    if (!val) {
        // null or undefined or ''(空文字) or 0 or false
        if (val !== 0 && val !== false) {
            return true;
        }
    } else if (typeof val === 'object') {
        // array or object
        return Object.keys(val).length === 0;
    }
    return false; // 値は空ではない
};

const isNumericDatatype = (type) => {
    const NUMERIC_TYPE = [
        'int',
        'num',
        'real',
        'float',
        'double',
        'long',
        'dec',
        'bit',
        'money',
        'r',
        'i',
    ];
    if (!type) return false;
    // convert to lower case before compare
    const lowerType = type.toLowerCase();
    for (let i = 0; i < NUMERIC_TYPE.length; i++) {
        if (lowerType.includes(NUMERIC_TYPE[i])) {
            return true;
        }
    }
    return false;
};

const jsonParse = (res) => {
    if (typeof res === 'string' || res instanceof String) {
        res = JSON.parse(res);
    }

    return res;
};

/**
 * Execute a function after milliseconds
 * @param {function(...[*|*[]]): any} func - function
 * @param {number} ms - milliseconds delay
 * @param {any[]} extendArgs
 * @return {function(...[*|*[]]): Promise<any> & { timeoutID: number?, disabled: boolean? }}
 */
function delay(func, ms, ...extendArgs) {
    /**
     * @param {...[*|*[]]} args
     * @return {Promise<any> & { timeoutID: number?, disabled: boolean? }}
     */
    return function inner(...args) {
        return new Promise((resolve) => {
            clearTimeout(inner.timeoutID);
            if (inner.disabled) {
                resolve();
                return;
            }

            inner.timeoutID = setTimeout(() => {
                const output = func.apply(this, [...args, ...extendArgs]);
                inner.timeoutID = undefined;
                resolve(output);
            }, ms || 0);
        });
    };
}

const docCookies = {
    getItem(sKey) {
        try {
            return decodeURIComponent(
                document.cookie.replace(
                    new RegExp(
                        `(?:(?:^|.*;)\\s*${encodeURIComponent(sKey).replace(/[\-\.\+\*]/g, '\\$&')}\\s*\\=\\s*([^;]*).*$)|^.*$`,
                    ),
                    '$1',
                ),
            );
        } catch (e) {
            return '';
        }
    },
    setItem(sKey, sValue, vEnd, sPath, sDomain, bSecure) {
        if (!sKey || /^(?:expires|max\-age|path|domain|secure)$/i.test(sKey)) {
            return false;
        }
        let sExpires = '';
        if (vEnd) {
            switch (vEnd.constructor) {
                case Number:
                    sExpires =
                        vEnd === Infinity
                            ? '; expires=Fri, 31 Dec 9999 23:59:59 GMT'
                            : `; max-age=${vEnd}`;
                    break;
                case String:
                    sExpires = `; expires=${vEnd}`;
                    break;
                case Date:
                    sExpires = `; expires=${vEnd.toUTCString()}`;
                    break;
            }
        }
        document.cookie = `${encodeURIComponent(sKey)}=${encodeURIComponent(sValue)}${sExpires}${sDomain ? `; domain=${sDomain}` : ''}; path=/${bSecure ? '; secure' : ''}`;
        return true;
    },
    removeItem(sKey, sPath, sDomain) {
        if (!sKey || !this.hasItem(sKey)) {
            return false;
        }
        document.cookie = `${encodeURIComponent(sKey)}=; expires=Thu, 01 Jan 1970 00:00:00 GMT${sDomain ? `; domain=${sDomain}` : ''}; path=/`;
        return true;
    },
    hasItem(sKey) {
        return new RegExp(
            `(?:^|;\\s*)${encodeURIComponent(sKey).replace(/[\-\.\+\*]/g, '\\$&')}\\s*\\=`,
        ).test(document.cookie);
    },
    getLocale() {
        return this.getItem('locale') || 'en';
    },
    isJaLocale() {
        return this.getLocale() === 'ja';
    },
};

// get or create node from object
const getOrCreateNewObj = (node, key, val = {}) => {
    if (!(key in node)) {
        node[key] = val;
    }
    return node[key];
};

// get or create node from object using array of keys
const getOrCreateNodeByKeys = (dictObj, keys, defaultVal = undefined) => {
    let node = dictObj;

    for (const key of keys) {
        // if key is a number , result will be wrong.
        // because it get a element in array instead a key in dictionary
        if (Array.isArray(node)) {
            return defaultVal;
        }

        if (node !== undefined && node !== null && typeof node === 'object') {
            if (node[key] === undefined) {
                node[key] = {};
            }
            node = node[key];
        } else {
            return defaultVal;
        }
    }
    return node;
};

// generate random string of length `len`
const generateRandomString = (len) =>
    'x'.repeat(len).replace(/[xy]/g, (c) => {
        const r = (Math.random() * 16) | 0;
        const v = c === 'x' ? r : (r & 0x3) | 0x8;
        return v.toString(16);
    });

const isArrayDuplicated = (array) => {
    if (!Array.isArray(array)) return false;
    return new Set(array).size !== array.length;
};

const findIndexes = (arr1, arr2) => {
    if (!arr1 || !arr2) return [];
    const output = [];
    arr1.forEach((e) => output.push(arr2.indexOf(e)));
    return output;
};

const getValueByIndexes = (indexes, targetArray, atlArray) => {
    if (!indexes) return [];
    const output = [];
    indexes.forEach((idx) => output.push(targetArray[idx] || atlArray[idx]));
    return output;
};

const setPageTitle = () => {
    // Set sub_title
    const subTitle =
        docCookies.getItem('sub_title') || COMMON_CONSTANT.DEFAULT_SUB_TITLE;
    $('#page-sub-title').text(subTitle);
    const currentPageTitle = $('title').text();
    const fullTitle = `${currentPageTitle} ${subTitle}`;
    // set page title
    $('title').html(fullTitle);
};

/**
 * Show or hide options that export data|image to clipboard base on secure context
 *
 * This function ONLY effects to show graph pages
 */
function showHideExportToClipboardOptions() {
    const $exportDataToClipboardRow = $('#exportDataToClipboardAcr').closest(
        'div.row',
    );
    const $exportImageToClipboardRow = $('#exportImageToClipboardAcr').closest(
        'div.row',
    );
    if (window.isSecureContext) {
        $exportDataToClipboardRow.show();
        $exportImageToClipboardRow.show();
    } else {
        $exportDataToClipboardRow.hide();
        $exportImageToClipboardRow.hide();
    }
}

const DEFAULT_LOCALE = 'ja';

$(() => {
    // show selected locale for pages
    const locale = docCookies.getItem('locale') || 'ja';
    // console.log(locale);
    $('#select-language').val(locale).trigger('change');

    // change locale and reload current page
    $('#select-language').change(function changeLocal() {
        let selectedLocale = $(this).children('option:selected').val();
        if (selectedLocale) {
            docCookies.setItem('locale', selectedLocale);
            const locale = docCookies.getItem('locale') || 'ja';
        } else {
            selectedLocale = DEFAULT_LOCALE;
        }
        $('#select-language').val(selectedLocale);
        window.location.reload(true);
    });

    // Set sub_title and page title
    setPageTitle();

    // press ESC to collapse floating dropdown
    document.addEventListener('keyup', keyPress);

    // avoid export menu auto close
    $(document).on('click', EXPORT_DOM.DROPDOWN_ID, function (e) {
        e.stopPropagation();
        if (e.target.closest('.dropdown-item')) {
            $(e.currentTarget).removeClass('show');
        }
    });

    showHideExportToClipboardOptions();
});

const validateNumericInput = (textbox) => {
    textbox.on('input', (eve) => {
        let { value } = eve.currentTarget;
        value = value.replace(/[^((0-9)|(０-９).\+\-)]+/gi, '');
        eve.currentTarget.value = value;
    });
    textbox.on('change', (eve) => {
        let { value } = eve.currentTarget;
        const replaceVal = /[０-９]/gi;
        value = value.replaceAll(replaceVal, (s) =>
            String.fromCharCode(s.charCodeAt(0) - 65248),
        );
        eve.currentTarget.value = value;
    });
};

const validateTargetPeriodInput = () => {
    // allow only integer for number of ridge lines
    validateNumericInput($(`#${CYCLIC_TERM.DIV_NUM}`));

    // allow only real for window length
    validateNumericInput($(`#${CYCLIC_TERM.WINDOW_LENGTH}`));

    // allow only real for interval
    validateNumericInput($(`#${CYCLIC_TERM.INTERVAL}`));

    validateNumericInput($(`input[name=${CYCLIC_TERM.DATA_NUMBER}]`));

    validateNumericInput($(`input[name=${CYCLIC_TERM.DIV_OFFSET}]`));

    validateNumericInput($(`input[name=${CYCLIC_TERM.RECENT_INTERVAL}]`));
};

const stringNormalization = (val) => {
    if (typeof val !== 'string') {
        return val;
    }

    let newVal = val;
    // normalization
    newVal = newVal.normalize('NFKC');

    // trim space
    newVal = trimBoth(newVal);

    // replace
    newVal = newVal.replace('°C', '℃');
    newVal = newVal.replace('°F', '℉');

    return newVal;
};

// convert input textbox element from Zenkaku to Hankaku on specific event
const handleInputTextZenToHanEvent = (
    event,
    afterOKCheckFn = null,
    callbackParams = null,
) => {
    event.currentTarget.value = stringNormalization(event.currentTarget.value);
    event.currentTarget.oldValue = event.currentTarget.value;

    // チェックOKの場合は、Callbackを呼び出す。
    if (afterOKCheckFn != null) {
        afterOKCheckFn(callbackParams);
    }
};

const setInputFilterH2Z = (element, unbind = false, ignoreWithAttr = []) => {
    for (const attr of ignoreWithAttr) {
        if (element.attr(attr)) {
            return;
        }
    }
    ['change'].forEach((eventName) => {
        if (unbind) {
            element.unbind(eventName);
        }
        element.on(eventName, handleInputTextZenToHanEvent);
    });
};

const assignValueToSelect2 = (select2Element, val) => {
    if (isEmpty(val)) return;
    if (typeof val === 'object') val = val.join(' ');
    let words = [val];
    const isMultipleselect2 = select2Element.attr('multiple');
    if (isMultipleselect2) {
        words = val.split(/[ ]+/);
        words = [...new Set(words)];
    }
    const matchOptions = [];
    for (const word of words) {
        if (!isEmpty(word)) {
            const found = select2Element
                .find('option')
                .filter((i, e) => $(e).val() === word).length;
            if (!found) {
                const newOption = new Option(word, word, false, false);
                select2Element.append(newOption).trigger('change');
            }
            matchOptions.push(word);
        }
    }
    select2Element.val(null).val(matchOptions).trigger('change');
};

const handleInputTextZenToHanEventSelect2 = (element) => {
    let val = element.val();

    if (!val) return;

    if (typeof val !== 'object') {
        val = [val];
    }
    val = val.join(' ');
    val = stringNormalization(val);
    assignValueToSelect2(element, val);
};

const setInputFilterH2ZSelect2 = (element, unbind = false) => {
    ['select2:close'].forEach((eventName) => {
        if (unbind) {
            element.unbind(eventName);
        }
        element.on(eventName, () => {
            handleInputTextZenToHanEventSelect2(element);
        });
    });
};

// convert input textbox element from Zenkaku to Hankaku on many events
const convertTextH2Z = (parent = null) => {
    let eles;
    if (parent) {
        eles = $(parent);
    } else {
        eles = $(document);
    }

    const alreadyConvertClass = '.already-convert-hankaku';
    eles.find('input, textarea')
        .not(alreadyConvertClass)
        .each(function () {
            if ($(this).attr('type') !== 'file') {
                setInputFilterH2Z($(this), false, ['multiple']);
                $(this).addClass(alreadyConvertClass.substr(1));
            }
        });

    eles.find('select')
        .not(alreadyConvertClass)
        .each(function () {
            const thisElement = $(this);
            if (thisElement.hasClass('convert-h2z')) {
                setInputFilterH2ZSelect2(thisElement, true);
                thisElement.addClass(alreadyConvertClass.substr(1));
            }
        });
};

// convert fullwidth special symbols to halfwidth katakana
// const replaceStringByArrayOfIndex = (str, src, dest) => {
//     const len = src.length;
//     for (let i = 0; i < len; i++) {
//         str = replaceAll(str, src[i], dest[i]);
//     }
//     return str;
// };

// const replaceAll = (target, from, to) => {
//     if (target.indexOf(from) < 0) {
//         return target;
//     }
//     return target.split(from).join(to);
// };

// const convertSymbolF2H = str => replaceStringByArrayOfIndex(str, F_SYMBOL, H_SYMBOL);

// const H_SYMBOL = new Array(
//     ',', '.', ':', ';', '!', '?', '"', "'", '`', '^', '~', '¯', '_',
//     '&', '@', '#', '%', '+', '-', '*', '=', '<', '>', '(', ')', '[', ']',
//     '{', '}', '(', ')', '|', '¦', '/', '\\', '¬', '$', '£', '¢', '₩', '¥', '｢', '｣',
// );

// const F_SYMBOL = new Array(
//     '，', '．', '：', '；', '！', '？', '＂', '＇', '｀', '＾', '～', '￣', '＿',
//     '＆', '＠', '＃', '％', '＋', '－', '＊', '＝', '＜', '＞', '（', '）', '［', '］',
//     '｛', '｝', '｟', '｠', '｜', '￤', '／', '＼', '￢', '＄', '￡', '￠', '￦', '￥', '「', '」',
// );
//
// const EnclosedNumbers = new Array('⓪', '①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨');
// const UnicodeNumber = new Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9');
//
// // convert half-width katakana to full-width katakana
// const convertKanaH2F = str => replaceStringByArrayOfIndex(str, H_KANA, F_KANA);
// const convertEnclosedNumber = str => replaceStringByArrayOfIndex(str, EnclosedNumbers, UnicodeNumber);
//
// const H_KANA = [
//     'ｶﾞ', 'ｷﾞ', 'ｸﾞ', 'ｹﾞ', 'ｺﾞ', 'ｻﾞ', 'ｼﾞ', 'ｽﾞ', 'ｾﾞ', 'ｿﾞ',
//     'ﾀﾞ', 'ﾁﾞ', 'ﾂﾞ', 'ﾃﾞ', 'ﾄﾞ', 'ﾊﾞ', 'ﾋﾞ', 'ﾌﾞ', 'ﾍﾞ', 'ﾎﾞ', 'ｳﾞ', // 濁音
//     'ﾊﾟ', 'ﾋﾟ', 'ﾌﾟ', 'ﾍﾟ', 'ﾎﾟ', // 半濁音
//     'ｧ', 'ｨ', 'ｩ', 'ｪ', 'ｫ', 'ｬ', 'ｭ', 'ｮ', 'ｯ', 'ｰ', 'ﾜ', // 小文字
//     'ｱ', 'ｲ', 'ｳ', 'ｴ', 'ｵ', 'ｶ', 'ｷ', 'ｸ', 'ｹ', 'ｺ', // 50音
//     'ｻ', 'ｼ', 'ｽ', 'ｾ', 'ｿ', 'ﾀ', 'ﾁ', 'ﾂ', 'ﾃ', 'ﾄ',
//     'ﾅ', 'ﾆ', 'ﾇ', 'ﾈ', 'ﾉ', 'ﾊ', 'ﾋ', 'ﾌ', 'ﾍ', 'ﾎ',
//     'ﾏ', 'ﾐ', 'ﾑ', 'ﾒ', 'ﾓ', 'ﾔ', 'ﾕ', 'ﾖ',
//     'ﾗ', 'ﾘ', 'ﾙ', 'ﾚ', 'ﾛ', 'ﾜ', 'ｲ', 'ｦ', 'ｴ', 'ﾝ', // 50音
// ];
//
// const F_KANA = ['ガ', 'ギ', 'グ', 'ゲ', 'ゴ', 'ザ', 'ジ', 'ズ', 'ゼ', 'ゾ',
//     'ダ', 'ヂ', 'ヅ', 'デ', 'ド', 'バ', 'ビ', 'ブ', 'ベ', 'ボ', 'ヴ', // 濁音
//     'パ', 'ピ', 'プ', 'ペ', 'ポ', // 半濁音
//     'ァ', 'ィ', 'ゥ', 'ェ', 'ォ', 'ャ', 'ュ', 'ョ', 'ッ', 'ー', 'ヮ', // 小文字
//     'ア', 'イ', 'ウ', 'エ', 'オ', 'カ', 'キ', 'ク', 'ケ', 'コ', // 50音
//     'サ', 'シ', 'ス', 'セ', 'ソ', 'タ', 'チ', 'ツ', 'テ', 'ト',
//     'ナ', 'ニ', 'ヌ', 'ネ', 'ノ', 'ハ', 'ヒ', 'フ', 'ヘ', 'ホ',
//     'マ', 'ミ', 'ム', 'メ', 'モ', 'ヤ', 'ユ', 'ヨ',
//     'ラ', 'リ', 'ル', 'レ', 'ロ', 'ワ', 'ヰ', 'ヲ', 'ヱ', 'ン', // 50音
// ];
//
// const removeUnexpectedChars = (str) => {
//     const acceptChars = [' ', ...H_KANA, ...F_KANA, ...H_SYMBOL, ...F_SYMBOL, ...EnclosedNumbers, ...UnicodeNumber];
//     const alphabets = new RegExp(/[0-9a-zA-Z０-９ａ-ｚＡ-Ｚ\u3040-\u309f\u4e00-\u9faf\u3400-\u4dbf]+/g);
//
//     let res = '';
//     [...str].forEach((char) => {
//         if (acceptChars.includes(char) || char.match(alphabets)) {
//             res += char;
//         }
//     });
//     return res;
// };

// gen Json for Db Config Data
const genJsonfromHTML = (
    searchFromHtml,
    jsonRootKey,
    valAlwaysArray = false,
) => {
    const result = {};
    const parentHtml =
        typeof searchFromHtml === 'string'
            ? $(`${searchFromHtml}`)
            : searchFromHtml;

    if (parentHtml.length === 1) {
        // root value is a json
        result[jsonRootKey] = {};
    } else {
        // root value is an array
        result[jsonRootKey] = [...Array(parentHtml.length)].map(() =>
            jsonParse('{}'),
        );
    }

    // closure
    const inner = (
        searchHtmlName,
        getHtmlValFunc = null,
        jsonKey = searchHtmlName,
    ) => {
        // loop parent HTML elements
        parentHtml.each((i, parentEle) => {
            const vals = [
                ...parentEle.querySelectorAll(`[name=${searchHtmlName}]`),
            ].map((e) =>
                getHtmlValFunc === null ? e.value : getHtmlValFunc(e),
            );
            // const vals = [...$(parentEle).find(`[name=${searchHtmlName}]`)].map(e => (getHtmlValFunc === null ? e.value : getHtmlValFunc(e)));

            let val;
            switch (vals.length) {
                case 0:
                    val = null;
                    break;
                case 1:
                    val = valAlwaysArray ? [...vals] : vals[0];
                    break;
                default:
                    val = [...vals];
            }

            // set value to root json
            if (parentHtml.length === 1) {
                result[jsonRootKey][jsonKey] = val;
            } else {
                result[jsonRootKey][i][jsonKey] = val;
            }
        });
        return result;
    };

    return inner;
};
const genJsonfromTbl = (
    searchFromHtml,
    jsonRootKey,
    valAlwaysArray = false,
) => {
    const result = {};
    const parentHtml =
        typeof searchFromHtml === 'string'
            ? $(`${searchFromHtml}`)
            : searchFromHtml;

    if (parentHtml.length === 1) {
        // root value is a json
        result[jsonRootKey] = {};
    } else {
        // root value is an array
        result[jsonRootKey] = [...Array(parentHtml.length)].map(() =>
            jsonParse('{}'),
        );
    }

    // closure
    const inner = (
        searchHtmlName,
        getHtmlValFunc = null,
        jsonKey = searchHtmlName,
    ) => {
        // loop parent HTML elements
        parentHtml.each((i, parentEle) => {
            const vals = [
                ...parentEle.querySelectorAll(`[name=${searchHtmlName}]`),
            ].map((e) =>
                getHtmlValFunc === null ? e.value : getHtmlValFunc(e),
            );
            // const vals = [...$(parentEle).find(`[name=${searchHtmlName}]`)].map(e => (getHtmlValFunc === null ? e.value : getHtmlValFunc(e)));

            let val;
            switch (vals.length) {
                case 0:
                    val = null;
                    break;
                case 1:
                    val = valAlwaysArray ? [...vals] : vals[0];
                    break;
                default:
                    val = [...vals];
            }

            // set value to root json
            if (parentHtml.length === 1) {
                result[jsonRootKey][jsonKey] = val;
            } else {
                result[jsonRootKey][i][jsonKey] = val;
            }
        });
        return result;
    };

    return inner;
};

// get node from dictionary
const getNode = (dictObj, keys, defaultVal = null) => {
    let node = dictObj;

    for (const key of keys) {
        // if key is a number , result will be wrong.
        // because it get a element in array instead a key in dictionary
        if (Array.isArray(node)) {
            return defaultVal;
        }

        if (node && typeof node === 'object') {
            node = node[key];
            // node is not a dictionary
            if (node === undefined) return defaultVal;
        } else {
            return defaultVal;
        }
    }

    return node;
};

// display message after press register buttons
const displayRegisterMessage = (
    alertID,
    flaskMessage = { message: '', is_error: false, is_warning: false },
) => {
    if (alertID === null || alertID === undefined) return;
    if (flaskMessage.message) {
        $(`${alertID}-content`).html(flaskMessage.message);
    }
    const alert = $(`${alertID}`);
    alert.removeClass('show');
    alert.removeClass('alert-success');
    alert.removeClass('alert-danger');
    alert.removeClass('alert-warning');
    if (flaskMessage.is_warning) {
        alert.css('display', 'block');
        alert.addClass('show alert-warning');
    } else if (!flaskMessage.is_error) {
        alert.css('display', 'block');
        alert.addClass('show alert-success');
    } else if (flaskMessage.is_error) {
        alert.css('display', 'block');
        alert.addClass('show alert-danger');
    }
    // setTimeout(() => {
    //     alert.css('display', 'none');
    // }, 5000);
};

const hideAlertMessages = () => {
    $('.alert.alert-dismissible.fade.show').css('display', 'none');
};

// delete closest element
const delClosestEle = (e, closestEle) => {
    e.closest(closestEle).remove();
};

const sidebarEles = {
    sid: '#sidebar',
    sidebarIds: '#sidebar, #content',
    collapseIn: '.collapse.in',
    aExpanded: 'a[aria-expanded=true]',
    dropdownToggle: '.side-dropdown-toggle',
    parentEles: 'span.nav-text, .sidebar-header a, .logo-header',
    ulList: 'ul.list-unstyled',
    cfgSm: 'cfgSm',
    sidebarCollapseId: '#sidebarCollapse',
    collapse: '.collapse',
};

const sidebarCollapse = () => {
    // toggle attribute plugin jquery
    $.fn.toggleAttr = function (attr, value) {
        return this.each(function () {
            const $this = $(this);
            $this.attr(attr) ? $this.removeAttr(attr) : $this.attr(attr, value);
        });
    };
    $(sidebarEles.sidebarIds).toggleClass('active');
    $(sidebarEles.collapseIn).toggleClass('in');
    // Set collapse for submenu
    $(sidebarEles.aExpanded).attr('aria-expanded', 'false');
    $(sidebarEles.dropdownToggle).toggleClass('noafter');
    $(sidebarEles.parentEles).toggleClass('hide');

    $(sidebarEles.dropdownToggle).each((k, e) => {
        if ($(e).not('collapsed')) {
            $(e).addClass('collapsed');
        }
    });
    $(sidebarEles.ulList).removeClass('show');

    // trigger resize window
    window.dispatchEvent(new Event('resize'));
};

const isSidebarOpen = () => {
    if ($(sidebarEles.sidebarIds).hasClass('active')) {
        return false;
    }
    return true;
};

const menuCollapse = () => {
    setTimeout(() => {
        $('#sidebar .collapse.show').collapse('hide');
    }, 500);
};

const closeSidebar = () => {
    menuCollapse();
    if (isSidebarOpen()) {
        sidebarCollapse();
    }
};

const beforeShowGraphCommon = (clearOnFlyFilter = true) => {
    closeSidebar();
    loadingShow(false, clearOnFlyFilter);
};

const fetchBackgroundJobs = (cb) => {
    fetchWithLog('/ap/api/setting/job', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
    })
        .then((response) => response.clone().json())
        .then((json) => {
            if (json) {
                cb(json);
            }
        })
        .catch((e) => {
            console.error(e);
            //
        });
};

const runTime = () => new Date().getTime();

const DATE_FORMAT_TZ = 'YYYY-MM-DD HH:mm:ss Z';
const TIME_FORMAT_TZ = 'HH:mm:ss Z';
const DATE_FORMAT_WITHOUT_TZ = 'YYYY-MM-DD HH:mm:ss';
const DATE_FORMAT = 'YYYY-MM-DD';
const DATE_PICKER_FORMAT = 'yy-mm-dd';
const DATE_FORMAT_VALIDATE = [
    'YYYY/MM/DD',
    'YYYY/M/DD',
    'YYYY/M/D',
    'YYYY/M/DD',
    'YYYY-MM-DD',
    'YYYY-M-DD',
    'YYYY-M-DD',
    'YYYY-M-D',
];
const TIME_FORMAT = 'HH:mm';
const TIME_FORMAT_VALIDATE = ['HH:mm', 'H:mm', 'HH:m', 'H:m'];

const TRACE_TIME_CONST = {
    DEFAULT: 'default',
    RECENT: 'recent',
    FROM: 'from',
    TO: 'to',
};
const TRACE_TIME_NAME = {
    PCA: 'testTraceTime',
};
const DATETIME_FORMAT_VALIDATE = DATE_FORMAT_VALIDATE.concat([
    DATE_FORMAT_TZ,
    DATE_FORMAT_WITHOUT_TZ,
    DATE_FORMAT,
    DATE_PICKER_FORMAT,
]);

// replace_date
const formatDate = (dt) => dt.replaceAll('/', '-');

const formatDateTime = (dt, fm = DATE_FORMAT_WITHOUT_TZ) => {
    let formatStr = fm;
    if (fm === DATE_FORMAT_WITHOUT_TZ) {
        const milliSeconds = checkDatetimeHasMilliseconds(dt);
        if (milliSeconds) {
            formatStr = DATE_FORMAT_WITHOUT_TZ + milliSeconds;
        }
    }
    return moment.utc(dt).local().format(formatStr);
};

const checkDatetimeHasMilliseconds = (datetimeStr) => {
    const match = datetimeStr.match(/(\.\d*)/);
    if (!match) {
        return false;
    }

    return '.SSS';
};

const detectLocalTimezone = () => {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
};

/* Binary search in JavaScript.
 * Returns the index of of the element in a sorted array or (-n-1)
 * where n is the insertion point for the new element.
 * Parameters:
 *     ar - A sorted array
 *     el - An element to search for
 *     compare_fn - A comparator function. The function takes two arguments: (a, b) and returns:
 *        a negative number  if a is less than b;
 *        0 if a is equal to b;
 *        a positive number of a is greater than b.
 * The array may contain duplicate elements. If there are more than one equal elements in the array,
 * the returned value can be the index of any one of the equal elements.
 */
const binarySearch = (ar, el, compareFn) => {
    let m = 0;
    let n = ar.length - 1;
    while (m <= n) {
        const k = (n + m) >> 1;
        const cmp = compareFn(el, ar[k]);
        if (cmp > 0) {
            m = k + 1;
        } else if (cmp < 0) {
            n = k - 1;
        } else {
            return k;
        }
    }
    return m - 1;
};

function calculateNiceRange(minPoint, maxPoint, maxTicks = 10) {
    const range = niceNum(maxPoint - minPoint, false);
    const tickSpacing = niceNum(range / (maxTicks - 1), true);
    const niceMin = Math.floor(minPoint / tickSpacing) * tickSpacing;
    const niceMax = Math.ceil(maxPoint / tickSpacing) * tickSpacing;

    return {
        niceMin,
        niceMax,
        tickSpacing,
    };
}

function niceNum(localRange, round) {
    let niceFraction;
    /** nice, rounded fraction */

    const exponent = Math.floor(Math.log10(localRange));
    const fraction = localRange / 10 ** exponent;

    if (round) {
        if (fraction < 1.5) {
            niceFraction = 1;
        } else if (fraction < 3) {
            niceFraction = 2;
        } else if (fraction < 7) {
            niceFraction = 5;
        } else {
            niceFraction = 10;
        }
    } else if (fraction <= 1) {
        niceFraction = 1;
    } else if (fraction <= 2) {
        niceFraction = 2;
    } else if (fraction <= 5) {
        niceFraction = 5;
    } else {
        niceFraction = 10;
    }
    return niceFraction * 10 ** exponent;
}

function getPrecision(n) {
    let loggedPrecision =
        1 - Math.floor(Math.log(Math.abs(n % 1))) / Math.log(10);
    loggedPrecision = Math.round(loggedPrecision);
    if (Number.isFinite(loggedPrecision)) {
        return loggedPrecision;
    }

    return 0;
}

// when user drag and drop items in tables -> save orders to db
const saveOrderToDB = (orderName, rowOrders = {}) => {
    ajaxWithLog({
        url: `/ap/api/setting/save_order/${orderName}`,
        data: JSON.stringify(rowOrders),
        dataType: 'json',
        type: 'POST',
        contentType: 'application/json',
        processData: false,
        success: (res) => {},
        error: (res) => {},
    });
};

// update order of tr when drag drop
const dragDropRowInTable = (() => {
    // html attribute for data order col
    const DATA_ORDER_ATTR = 'data-order';

    // get values of order columns
    const getValuesOfOrderCols = (ele) =>
        $(ele).attr('id') ||
        [...$(ele).find(`[${DATA_ORDER_ATTR}]`)].map((e) => e.value).join('_');

    const getValuesOfOrderColsMasterConfig = (ele) =>
        [...$(ele).find(`[${DATA_ORDER_ATTR}]`)]
            .map((e) => e.value)
            .join('_') || $(ele).attr('id');

    // fix width of tr when drag drop
    const fixHelper = (_v, ui) => {
        ui.children().each(function () {
            $(this).width($(this).width());
        });
        return ui;
    };

    // get values of order columns
    const getValuesOfOrderColsAndDsId = (ele) =>
        $(ele).attr('data-ds-id') ||
        [...$(ele).find(`[${DATA_ORDER_ATTR}]`)].map((e) => e.value).join('_');

    const getValuesOfOrderColsAndDsTableId = (ele) =>
        $(ele).attr('data-datatable-id') ||
        [...$(ele).find(`[${DATA_ORDER_ATTR}]`)].map((e) => e.value).join('_');

    const getValuesOfOrderColsAndDsProcId = (ele) =>
        $(ele).attr('data-proc-id') ||
        [...$(ele).find(`[${DATA_ORDER_ATTR}]`)].map((e) => e.value).join('_');

    const getValuesOfOrderColsAndDsCfgLineId = (ele) =>
        $(ele).attr('data-cfgfilterline-id') ||
        [...$(ele).find(`[${DATA_ORDER_ATTR}]`)].map((e) => e.value).join('_');

    // Update LocalStorage for ordering
    const setItemLocalStorage = (tblBody, prefix = null) => {
        const tblEle = tblBody.parentNode;
        let dicRowId;
        let localStorageId = tblEle.id;
        // if (prefix) {
        //     localStorageId = `${prefix}_${tblEle.id}`;
        // }

        if (localStorageId === 'tblDbConfig') {
            dicRowId = Object.fromEntries(
                [...tblBody.rows].map((ele, i) => [
                    getValuesOfOrderColsAndDsId(ele),
                    i,
                ]),
            );
        } else if (localStorageId === 'tblDataTableConfig') {
            dicRowId = Object.fromEntries(
                [...tblBody.rows].map((ele, i) => [
                    getValuesOfOrderColsAndDsTableId(ele),
                    i,
                ]),
            );
        } else if (localStorageId === 'tblProcConfig') {
            dicRowId = Object.fromEntries(
                [...tblBody.rows].map((ele, i) => [
                    getValuesOfOrderColsAndDsProcId(ele),
                    i,
                ]),
            );
        } else {
            dicRowId = Object.fromEntries(
                [...tblBody.rows].map((ele, i) => [
                    getValuesOfOrderColsAndDsCfgLineId(ele),
                    i,
                ]),
            );
        }

        localStorage.setItem(localStorageId, JSON.stringify(dicRowId));

        // save orders to db
        saveOrderToDB((orderName = localStorageId), (rowOrders = dicRowId));
    };

    // Update LocalStorage for ordering
    const updateOrder = (event) => {
        const tblBody = event.target;
        setItemLocalStorage(tblBody);
        updateTableRowNumber(null, null, tblBody);
    };

    const sortFunction = (a, b, orderDic) => {
        const keyA = orderDic[getValuesOfOrderCols(a)];
        const keyB = orderDic[getValuesOfOrderCols(b)];
        if (keyA > keyB) return 1;
        if (keyA < keyB) return -1;
        return 0;
    };

    const sortMasterConfigRowFunction = (a, b, orderDic) => {
        const keyA = orderDic[getValuesOfOrderColsMasterConfig(a)];
        const keyB = orderDic[getValuesOfOrderColsMasterConfig(b)];
        if (keyA > keyB) return 1;
        if (keyA < keyB) return -1;
        return 0;
    };

    // sort tr in table follow localStorage data
    const sortRowInTable = (localStorageId, prefix = null) => {
        const tbody = $(`#${localStorageId}`).children('tbody');
        const rows = tbody.find('tr').get();

        // if (prefix) {
        //     localStorageId = `${prefix}_${localStorageId}`;
        // }

        const orderDic = jsonParse(localStorage.getItem(localStorageId));

        // return if no order data
        if (!orderDic) {
            return;
        }

        if (localStorageId === 'tblVisualConfig') {
            rows.sort((a, b) => sortMasterConfigRowFunction(a, b, orderDic));
        } else {
            rows.sort((a, b) => sortFunction(a, b, orderDic));
        }
        rows.forEach((row) => tbody.append(row));

        updateTableRowNumber(localStorageId); // TODO
    };

    // return public function
    return {
        DATA_ORDER_ATTR,
        fixHelper,
        updateOrder,
        sortRowInTable,
        setItemLocalStorage,
    };
})();

const stickyHeaders = (() => {
    jQuery.expr.filters.offscreen = (el) => {
        const rect = el.getBoundingClientRect();
        return (
            rect.x + rect.width < 0 ||
            rect.y + rect.height < 0 ||
            rect.x > window.innerWidth ||
            rect.y > window.innerHeight
        );
    };
    const $window = $(window);
    let $stickies;

    const whenScrolling = () => {
        $stickies.each((i, e) => {
            const $cardParent = $(e).parents().filter('form').parent().parent();
            const $thisSticky = $(e);
            const $stickyPosition = $thisSticky.data('originalPosition');

            if (
                $stickyPosition <= $window.scrollTop() &&
                !$cardParent.is(':offscreen')
            ) {
                const $nextSticky = $stickies.eq(i + 1);
                const $nextStickyPosition =
                    $nextSticky.data('originalPosition') -
                    $thisSticky.data('originalHeight');

                $thisSticky.addClass('btn-fixed');

                if (
                    $nextSticky.length > 0 &&
                    $thisSticky.offset().top >= $nextStickyPosition
                ) {
                    $thisSticky
                        .addClass('absolute')
                        .css('top', $nextStickyPosition);
                }
            } else {
                const $prevSticky = $stickies.eq(i - 1);

                $thisSticky.removeClass('btn-fixed');

                if (
                    $prevSticky.length > 0 &&
                    $window.scrollTop() <=
                        $thisSticky.data('originalPosition') -
                            $thisSticky.data('originalHeight')
                ) {
                    $prevSticky.removeClass('absolute').removeAttr('style');
                }
            }
        });
    };
    const load = (stickies) => {
        if (
            typeof stickies === 'object' &&
            stickies instanceof jQuery &&
            stickies.length > 0
        ) {
            let $originWH = $(document).height();
            $stickies = stickies.each((_v, e) => {
                const $thisSticky = $(e).wrap('<div class="btnWrap">');

                $thisSticky
                    .data('originalPosition', $thisSticky.offset().top)
                    .data('originalHeight', $thisSticky.outerHeight());
            });

            $window.off('scroll.stickies').on('scroll.stickies', () => {
                // re-calc position
                const $newWH = $(document).height();
                if ($newWH !== $originWH) {
                    $stickies = stickies.each((_v, e) => {
                        $(e).data('originalPosition', $(e).offset().top);
                    });
                    $originWH = $newWH;
                }
                whenScrolling();
            });
        }
    };

    const divStickerOnLoad = () => {
        console.log('div sticky..');
    };
    return {
        load,
        divStickerOnLoad,
    };
})();

// TODO refactor scrollFloatingElement and stickyHeaders

const scrollFloatingElement = (() => {
    jQuery.expr.filters.offscreen = (el) => {
        const rect = el.getBoundingClientRect();
        return (
            rect.x + rect.width < 0 ||
            rect.y + rect.height < 0 ||
            rect.x > window.innerWidth ||
            rect.y > window.innerHeight
        );
    };
    const $window = $(window);
    let $stickies;
    let $adjustCSSClass;
    let $callBackFunc;

    const whenScrolling = () => {
        $stickies.each((i, e) => {
            const $cardParent = $(e).parents().filter('form').parent().parent();
            const $thisSticky = $(e);
            const $stickyPosition = $thisSticky.data('originalPosition');

            if (
                $stickyPosition <= $window.scrollTop() &&
                !$cardParent.is(':offscreen')
            ) {
                const $nextSticky = $stickies.eq(i + 1);
                const $nextStickyPosition =
                    $nextSticky.data('originalPosition') -
                    $thisSticky.data('originalHeight');

                // add custom css
                $thisSticky.addClass($adjustCSSClass);

                if (
                    $nextSticky.length > 0 &&
                    $thisSticky.offset().top >= $nextStickyPosition
                ) {
                    $thisSticky
                        .addClass('absolute')
                        .css('top', $nextStickyPosition);
                }

                if ($callBackFunc) {
                    $callBackFunc($thisSticky);
                }
            } else {
                const $prevSticky = $stickies.eq(i - 1);

                // remove custom css
                $thisSticky.removeClass($adjustCSSClass);

                if (
                    $prevSticky.length > 0 &&
                    $window.scrollTop() <=
                        $thisSticky.data('originalPosition') -
                            $thisSticky.data('originalHeight')
                ) {
                    $prevSticky.removeClass('absolute').removeAttr('style');
                }
            }
        });
    };
    const load = (stickies, adjustCSSClass = '', callBackFunc = null) => {
        if (
            typeof stickies === 'object' &&
            stickies instanceof jQuery &&
            stickies.length > 0
        ) {
            let $originWH = $(document).height();
            $stickies = stickies.each((_v, e) => {
                const $thisSticky = $(e).wrap('<div class="">');

                $thisSticky
                    .data('originalPosition', $thisSticky.offset().top)
                    .data('originalHeight', $thisSticky.outerHeight());
            });
            $adjustCSSClass = adjustCSSClass;
            $callBackFunc = callBackFunc;

            $window.off('scroll.stickies').on('scroll.stickies', () => {
                // re-calc position
                const $newWH = $(document).height();
                if ($newWH !== $originWH) {
                    $stickies = stickies.each((_v, e) => {
                        $(e).data('originalPosition', $(e).offset().top);
                    });
                    $originWH = $newWH;
                }
                whenScrolling();
            });
        }
    };
    return {
        load,
    };
})();

const showToastrMsg = (msgContent, level = MESSAGE_LEVEL.WARN) => {
    if (isSSEListening) return;
    if (!msgContent) {
        return;
    }

    if ($closeAllToast) {
        toastr.clear($closeAllToast);
        $closeAllToast = null;
    }

    toastr.options = {
        closeButton: true,
        debug: false,
        newestOnTop: true,
        progressBar: true,
        positionClass: 'toast-bottom-right',
        onclick: null,
        showDuration: 500,
        hideDuration: 200,
        timeOut: 10000,
        extendedTimeOut: 500,
        showEasing: 'swing',
        hideEasing: 'linear',
        showMethod: 'fadeIn',
        hideMethod: 'fadeOut',
        onHidden: () => {
            const toastLength = $('.toast').length;
            if (toastLength <= 1) {
                window.toastr.clear();
            }
        },
    };

    let $toast = null;
    if (level === MESSAGE_LEVEL.ERROR) {
        $toast = toastr.error(msgContent, '');
    } else if (level === MESSAGE_LEVEL.INFO) {
        $toast = toastr.info(msgContent, '');
    } else {
        $toast = toastr.warning(msgContent, '');
    }
    $toastlast = $toast;
    setTimeout(() => {
        if ($('.close-all-toast').length <= 0) {
            $closeAllToast = closeAllToast();
        }
    }, 100);
};

const closeAllToast = () => {
    toastr.options = {
        closeButton: true,
        debug: false,
        newestOnTop: false,
        progressBar: true,
        positionClass: 'toast-top-full-width',
        onclick: () => {
            window.toastr.clear();
        },
        showIcon: false,
        showDuration: 0,
        hideDuration: 0,
        timeOut: 0,
        extendedTimeOut: 0,
        showEasing: 'swing',
        hideEasing: 'linear',
        showMethod: 'fadeIn',
        hideMethod: 'fadeOut',
    };

    return toastr
        .info(document.getElementById('i18nCloseAll').textContent)
        .addClass('close-all-toast');
};

// show toastr msg to warn about abnormal result
const showToastrAnomalGraph = () => {
    const i18nTexts = {
        abnormalGraphShow: $('#i18nAbnormalGraphShow')
            .text()
            .split('BREAK_LINE')
            .join('<br>'),
    };

    const msgContent = `<p>${i18nTexts.abnormalGraphShow}</p>`;

    showToastrMsg(msgContent, MESSAGE_LEVEL.ERROR);
};

const showToastrStartedCSV = (isCSV = true) => {
    const i18nTexts = {
        msgCSV: $('#i18nStartedCSVDownload').text(),
        msgTSV: $('#i18nStartedTSVDownload').text(),
    };

    const msgContent = `<p>${isCSV ? i18nTexts.msgCSV : i18nTexts.msgTSV}</p>`;

    showToastrMsg(msgContent, MESSAGE_LEVEL.INFO);
};

const syncTraceDateTime = (parentId = '', dtNames = {}, dtValues = {}) => {
    /*
        Synchronize datetime of query with UI date-range
    */
    if (isEmpty(parentId)) return;

    const startDate = dtNames.START_DATE;
    const startTime = dtNames.START_TIME;
    const endDate = dtNames.END_DATE;
    const endTime = dtNames.END_TIME;

    const startDateVal = dtValues.START_DATE;
    const startTimeVal = dtValues.START_TIME;
    const endDateVal = dtValues.END_DATE;
    const endTimeVal = dtValues.END_TIME;

    if (!isEmpty(startDate) && !isEmpty(startDateVal)) {
        $(`#${parentId} input[name=${startDate}]`).first().val(startDateVal);
    }
    if (!isEmpty(startTime) && !isEmpty(startTimeVal)) {
        $(`#${parentId} input[name=${startTime}]`).first().val(startTimeVal);
    }
    if (!isEmpty(endDate) && !isEmpty(endDateVal)) {
        $(`#${parentId} input[name=${endDate}]`).first().val(endDateVal);
    }
    if (!isEmpty(endTime) && !isEmpty(endTimeVal)) {
        $(`#${parentId} input[name=${endTime}]`).first().val(endTimeVal);
    }
};

const syncTraceDateTimeRange = (parentId = '', dtNames = {}, dtValues = {}) => {
    /*
        Synchronize datetime of query with UI date-range
    */
    if (isEmpty(parentId)) return;

    const datetimeRange = dtNames.DATETIME_RANGE_PICKER;
    const datetimeRangeVal = dtValues.DATETIME_RANGE_PICKER;

    if (!isEmpty(datetimeRange) && !isEmpty(datetimeRangeVal)) {
        $(`#${parentId} input[name=${datetimeRange}]`)
            .first()
            .val(datetimeRangeVal);
    }
};

const toUTCDateTime = (localDate, localTime, withDateTime = false) => {
    if (!withDateTime && (isEmpty(localDate) || isEmpty(localTime)))
        return { date: localDate, time: localTime };

    const datetime = withDateTime ? localDate : `${localDate} ${localTime}`;
    const utcDT = moment.utc(moment(datetime, `${DATE_FORMAT} ${TIME_FORMAT}`));
    if (utcDT.isValid()) {
        if (withDateTime) {
            return utcDT.format(DATE_TIME_FMT);
        }
        return {
            date: utcDT.format(DATE_FORMAT),
            time: utcDT.format(TIME_FORMAT),
        };
    }
    return { date: localDate, time: localTime };
};

const getColumnName = (endProcName, getVal) => {
    const col = procConfigs[endProcName].getColumnById(getVal) || {};
    return col.shown_name || getVal;
};

const isCycleTimeCol = (endProcId, colId) => {
    const col = procConfigs[endProcId].getColumnById(colId) || {};
    const isCT = col.data_type === DataTypes.DATETIME.name || false;
    return isCT;
};

// retrieve cycle time cols
const getCTCols = (endProcName) => {
    const cols = procConfigs[endProcName].getCTColumns() || {};
    return cols || null;
};

const createDatetime = (v) => {
    if (v === 'min') {
        return new Date('1970-01-01');
    }
    if (v === 'max') {
        return new Date('10000-01-01');
    }
    return new Date(v);
};

const createIndex = (v) => {
    if (v === 'min') {
        return 0;
    }
    if (v === 'max') {
        return 999999999;
    }
    return v;
};

const getChartInfo = (plotdata, xAxisOption = 'TIME', filterCond = null) => {
    let chartInfos = plotdata.chart_infos || [];
    let chartInfosOrg = plotdata.chart_infos_org || [];

    if (xAxisOption === 'INDEX') {
        chartInfos = plotdata.chart_infos_ci || [];
        chartInfosOrg = plotdata.chart_infos_org_ci || [];
        return [chartInfos, chartInfosOrg];
    }

    // Filter when data has facet and filter condition threshold is same value -> remove once
    if (filterCond) {
        let isFacetHasFilterConditionValue = false;
        for (const facet of filterCond) {
            for (const chartInfo of [...chartInfos, ...chartInfosOrg]) {
                if (chartInfo.name && facet === chartInfo.name) {
                    isFacetHasFilterConditionValue = true;
                    break;
                }
            }
        }

        if (!isFacetHasFilterConditionValue) {
            return [chartInfos, chartInfosOrg];
        }

        chartInfos = chartInfos.filter((settingInfo) =>
            settingInfo.name ? filterCond.includes(settingInfo.name) : true,
        );
        chartInfosOrg = chartInfosOrg.filter((settingInfo) =>
            settingInfo.name ? filterCond.includes(settingInfo.name) : true,
        );
    }

    return [chartInfos, chartInfosOrg];
};

const chooseLatestThresholds = (
    chartInfos = [],
    chartInfosOrg = [],
    clickedIdx = null,
    convertFunc = createDatetime,
) => {
    if (isEmpty(chartInfos)) return [{}, 0];

    // no click -> no point -> no time
    // if chartInfosOrg empty -> don't use chartInfosOrg
    if (isEmpty(clickedIdx)) {
        let latestChartInfo = chartInfos[0];
        let latestIndex = 0;
        for (const idx in chartInfos) {
            const chartInfo = chartInfos[idx];
            const latestActTo = latestChartInfo['act-to'];
            const actTo = chartInfo['act-to'];
            if (
                isEmpty(actTo) ||
                (!isEmpty(latestActTo) && actTo > latestActTo)
            ) {
                latestChartInfo = chartInfo;
                latestIndex = idx;
            }
        }
        return [latestChartInfo, latestIndex];
    }

    const defaultActFrom = convertFunc('min');
    const defaultActTo = convertFunc('max');
    const sensorDateTime = convertFunc(clickedIdx);

    let latestChartInfoOrg = null;
    let latestIndexOrg = null;
    for (const idx in chartInfosOrg) {
        const chartInfo = chartInfosOrg[idx];
        const actFrom = isEmpty(chartInfo['act-from'])
            ? defaultActFrom
            : convertFunc(chartInfo['act-from']);
        const actTo = isEmpty(chartInfo['act-to'])
            ? defaultActTo
            : convertFunc(chartInfo['act-to']);
        let relevantChartInfo = false;
        if (actFrom <= sensorDateTime && sensorDateTime <= actTo) {
            relevantChartInfo = true;
        }
        if (relevantChartInfo) {
            if (!latestChartInfoOrg) {
                latestChartInfoOrg = chartInfo;
                latestIndexOrg = idx;
            } else {
                const latestActTo = isEmpty(latestChartInfoOrg['act-to'])
                    ? defaultActTo
                    : convertFunc(latestChartInfoOrg['act-to']);
                if (latestActTo < actTo) {
                    latestChartInfoOrg = chartInfo;
                    latestIndexOrg = idx;
                }
            }
        }
    }
    const latestIndex = latestIndexOrg || 0;
    const latestChartInfo = chartInfosOrg[latestIndex] || {};
    return [latestChartInfo, latestIndex];
};

const findMinMax = (arr = [], compareDate = false) => {
    let ymin = null;
    let ymax = null;
    if (arr && arr.length) {
        const filteredArray = arr.filter(
            (v) =>
                (!_.isObject(v) || compareDate) &&
                v !== null &&
                v !== undefined,
        );
        for (const e of filteredArray) {
            if (ymin === null) {
                ymin = e;
                ymax = e;
            } else if (e < ymin) {
                ymin = e;
            } else if (e > ymax) {
                ymax = e;
            }
        }
    }
    return [ymin, ymax];
};

const formatResultMulti = (data) => {
    let classAttr = $(data.element).attr('class');
    const hasClass = typeof classAttr !== 'undefined';
    classAttr = hasClass ? ` ${classAttr}` : '';

    let columnName = $(data.element).attr('title');
    if (columnName === undefined) {
        columnName = '---';
    }

    const $result = $(
        `${
            '<div class="row">' + '<div class="col-md-6 col-xs-6'
        }${classAttr}">${columnName}</div>` +
            `<div class="col-md-6 col-xs-6${classAttr}">${data.text}</div>` +
            '</div>',
    );
    return $result;
};

const matchCustom = (params, data) => {
    if (typeof params.term === 'undefined') {
        return data;
    }

    // If there are no search terms, return all of the data
    if ($.trim(params.term) === '') {
        return data;
    }

    // Do not display the item if there is no 'text' property
    if (typeof data.text === 'undefined') {
        return null;
    }

    // `params.term` should be the term that is used for searching
    // `data.text` is the text that is displayed for the data object
    params.term = stringNormalization(params.term);
    params.term = params.term.toLowerCase();
    const dataText = data.text.toLowerCase();
    const dataTitle = data.title.toLowerCase();
    if (
        dataText.indexOf(params.term) > -1 ||
        dataTitle.indexOf(params.term) > -1
    ) {
        const modifiedData = $.extend({}, data, true);

        // You can return modified objects from here
        // This includes matching the `children` how you want in nested data sets
        return modifiedData;
    }

    // Return `null` if the term should not be displayed
    return null;
};

const setSelect2Selection = (parent = null, additionalOption = {}) => {
    let eles;
    if (parent) {
        eles = $(parent);
    } else {
        eles = $(document);
    }

    const commonOption = {
        placeholder: `${i18nCommon.search}...`,
        allowClear: true,
        matcher: matchCustom,
        dropdownAutoWidth: true,
        width: 'auto',
        language: {
            noResults: function (params) {
                return i18nCommon.notApplicable;
            },
        },
    };

    const nColCls = 'select-n-columns';
    // const alreadyConvertCls = 'already-convert-select2';
    const dicOptions = {
        ...commonOption,
        ...additionalOption,
    };

    const dicNColsOptions = {
        ...commonOption,
        templateResult: formatResultMulti,
        ...additionalOption,
    };

    // single select2
    eles.find('select.select2-selection--single:not([hidden])').each(
        function () {
            const ele = $(this);
            let select2El = null;

            if (ele.attr('data-allow-clear')) {
                Object.assign(dicNColsOptions, { allowClear: true });
                Object.assign(dicOptions, { allowClear: true });
            }
            if (ele.hasClass(nColCls)) {
                select2El = ele.select2(dicNColsOptions);
            } else {
                select2El = ele.select2(dicOptions);
            }

            // Add placeholder in search input
            $(ele)
                .data('select2')
                .$dropdown.find(':input.select2-search__field')
                .attr('placeholder', i18nCommon.search + '...');

            resizeListOptionSelect2(select2El);
        },
    );
};

const uniq = (vals) => [...new Set(vals)];

const getPearsonCorrelation = (_x, _y) => {
    let shortestArrayLength = 0;
    const x = _x.filter((i) => Number.isFinite(i));
    const y = _y.filter((i) => Number.isFinite(i));
    if (x.length === y.length) {
        shortestArrayLength = x.length;
    } else if (x.length > y.length) {
        shortestArrayLength = y.length;
        // console.error(`x has more items in it, the last ${x.length - shortestArrayLength} item(s) will be ignored`);
    } else {
        shortestArrayLength = x.length;
        // console.error(`y has more items in it, the last ${y.length - shortestArrayLength} item(s) will be ignored`);
    }

    const xy = [];
    const x2 = [];
    const y2 = [];

    for (let i = 0; i < shortestArrayLength; i++) {
        if (!isEmpty(x[i]) && !isEmpty(y[i])) {
            xy.push(x[i] * y[i]);
            x2.push(x[i] * x[i]);
            y2.push(y[i] * y[i]);
        }
    }

    let sumX = 0;
    let sumY = 0;
    let sumXY = 0;
    let sumX2 = 0;
    let sumY2 = 0;

    for (let i = 0; i < shortestArrayLength; i++) {
        if (!isEmpty(x[i]) && !isEmpty(y[i])) {
            sumX += x[i];
            sumY += y[i];
            sumXY += xy[i];
            sumX2 += x2[i];
            sumY2 += y2[i];
        }
    }

    const step1 = shortestArrayLength * sumXY - sumX * sumY;
    const step2 = shortestArrayLength * sumX2 - sumX * sumX;
    const step3 = shortestArrayLength * sumY2 - sumY * sumY;
    const step4 = Math.sqrt(step2 * step3);
    const answer = step1 / step4;

    if (Number.isNaN(answer) || !Number.isFinite(answer)) {
        return 'N/A';
    }
    return answer.toFixed(3);
};

const getCorrelation = (corrMatrix, sensorID, targetID) => {
    // console.log(corrMatrix);
    const corrs = corrMatrix.corr;
    if (Object.keys(corrs).length) {
        const corr = corrs[sensorID][targetID];
        return corr || 0;
    }
    return 0;
};

const genPlotlyIconSettings = () => ({
    modeBarButtonsToRemove: [
        'zoom2d',
        'pan2d',
        'select2d',
        'lasso2d',
        'zoomIn2d',
        'zoomOut2d',
        'hoverClosestCartesian',
        'hoverCompareCartesian',
        'toggleSpikelines',
        'sendDataToCloud',
    ],
    displaylogo: false,
});

const initializeDateTime = (dtElements = {}, syncStartEnd = true) => {
    let dateDiff = 0;
    const startDateElement =
        dtElements.START_DATE || $('input[name="START_DATE"]');
    const endDateElement = dtElements.END_DATE || $('input[name="END_DATE"]');
    const startTimeElement =
        dtElements.START_TIME || $('input[name="START_TIME"]');
    const endTimeElement = dtElements.END_TIME || $('input[name="END_TIME"]');

    // set default time
    if (formElements.radioDefaultInterval) {
        formElements.radioDefaultInterval.prop('checked', true);
    }

    // set date format
    $('.datepicker').datepicker({
        dateFormat: DATE_PICKER_FORMAT,
    });

    const timeSetting = {
        regional: 'ja',
        controlType: 'select',
        oneLine: true,
        timeFormat: 'HH:mm',
    };
    startDateElement.datepicker('setDate', '0');
    startTimeElement.timepicker(timeSetting).val('07:00');
    // use old value for validation
    startDateElement.attr('data-current-val', startDateElement.val());
    startTimeElement.attr('data-current-val', startTimeElement.val());

    // set end_date value to start_date value when changing start_date
    startDateElement.on('change', (event) => {
        validateDateTime(event, DATE_FORMAT_VALIDATE);
        if (syncStartEnd) {
            // update end_date to value of start_date
            const startDate = startDateElement.val();
            const mStartDate = moment(startDate, DATE_FORMAT);
            const newEndDate = mStartDate
                .add(dateDiff, 'days')
                .format(DATE_FORMAT);
            endDateElement.val(newEndDate);
            endDateElement.attr('data-current-val', newEndDate);
            // todo: update datetime range span
        }
    });
    if (endDateElement && endDateElement.length === 1) {
        endDateElement.datepicker('setDate', '0');
        endTimeElement.timepicker(timeSetting).val('19:00');
        endDateElement.attr('data-current-val', endDateElement.val());
        endTimeElement.attr('data-current-val', endTimeElement.val());

        // validate end_date
        endDateElement.on('change', (event) => {
            validateDateTime(event, DATE_FORMAT_VALIDATE);
            if (syncStartEnd) {
                // update date diffence between start date and end date
                const startDate = startDateElement.val();
                const mStartDate = moment(startDate, DATE_FORMAT);
                const mEndDate = moment(endDateElement.val(), DATE_FORMAT);
                dateDiff = mEndDate.diff(mStartDate, 'days');
            }
        });
        // validate end_time
        endTimeElement.on('change', (event) => {
            validateDateTime(event, TIME_FORMAT_VALIDATE);
        });
    }

    // validate start_time
    startTimeElement.on('change', (event) => {
        validateDateTime(event, TIME_FORMAT_VALIDATE);
    });
};

const validateDateTime = (event, dtFormat) => {
    const curTarget = event.target;
    const newValue = event.target.value;
    if (newValue === curTarget.getAttribute('data-current-val')) {
        return;
    }
    const { currentVal } = event.target.dataset;
    if (!moment(newValue, dtFormat, true).isValid()) {
        event.target.value = currentVal;
    } else {
        event.target.dataset.currentVal = newValue;
    }
};

const adjustMinMax1Percent = (minY, maxY) => [
    minY - 0.01 * Math.abs(minY),
    maxY + 0.01 * Math.abs(maxY),
];

const createHistogramParams = (
    scaleOption,
    arrayY,
    setYMin,
    setYMax,
    commonMinY,
    commonMaxY,
    latestChartInfo,
    corrSummary,
) => {
    //  max/minY based on scaleOption
    let maxY = getNode(corrSummary, ['non_parametric', 'upper_range']);
    let minY = getNode(corrSummary, ['non_parametric', 'lower_range']);
    if (scaleOption === '1') {
        const cfgYMax = latestChartInfo['y-max'];
        const cfgYMin = latestChartInfo['y-min'];
        if (cfgYMax) minY = cfgYMax;
        if (cfgYMin) maxY = cfgYMin;
    } else if (scaleOption === '2') {
        minY = commonMinY;
        maxY = commonMaxY;
    } else if (scaleOption === '3') {
        const threshHigh = latestChartInfo['thresh-high'];
        const threshLow = latestChartInfo['thresh-low'];
        if (
            !isEmpty(threshHigh) &&
            !isEmpty(threshLow) &&
            threshHigh !== threshLow
        ) {
            maxY = threshHigh + Math.abs(threshHigh - threshLow) * 0.125;
            minY = threshLow - Math.abs(threshHigh - threshLow) * 0.125;
        }
    } else if (scaleOption === '5') {
        minY = setYMin;
        maxY = setYMax;
    }

    if (minY === maxY) {
        [minY, maxY] = adjustMinMax1Percent(minY, maxY);
    }
    if (minY > maxY) {
        // rare case, i saw long time ago
        const temp = minY;
        minY = maxY;
        maxY = temp;
    }

    const globalRange = [minY, maxY];
    const customBinSize =
        Math.max(
            (globalRange[1] - globalRange[0]) / 128,
            Math.round(setYMax - setYMin) / 128,
        ) || 1;

    return [minY, maxY, globalRange, customBinSize];
};

const colorErrorCells = (jexcelDivId, errorCells, requiredErrorEles = []) => {
    if (isEmpty(errorCells)) return;

    const styleParams = errorCells.reduce(
        (a, b) => ({ ...a, [b]: 'color:red;' }),
        {},
    );
    document.getElementById(jexcelDivId).jspreadsheet.setStyle(styleParams);
    if (requiredErrorEles.length) {
        const emptyCellsParams = requiredErrorEles.reduce(
            (a, b) => ({ ...a, [b]: 'background-color: red;' }),
            {},
        );
        document
            .getElementById(jexcelDivId)
            .jspreadsheet.setStyle(emptyCellsParams);
    }
};

const colorErrorCheckboxCells = (jexcelDivId, errorCells) => {
    if (isEmpty(errorCells)) return;

    const styleParams = errorCells.reduce(
        (a, b) => ({ ...a, [b]: 'background-color: red;' }),
        {},
    );
    document.getElementById(jexcelDivId).jspreadsheet.setStyle(styleParams);
};

const showGAToastr = (errors) => {
    if (!errors) {
        return;
    }

    const msgContent = `<p>${i18n.gaUnable}</p>
    <p>${i18n.gaCheckConnect}</p>`;
    showToastrMsg(msgContent, MESSAGE_LEVEL.ERROR);
};

const getSelectedItems = (
    isCategoryItem = false,
    selectParent = $(formElements.endProcSelectedItem),
) => {
    const selectedItems = [];

    let allSelected = [];
    if (isCategoryItem) {
        allSelected = $(formElements.endProcCateSelectedItem);
    } else {
        allSelected = selectParent;
    }

    if (allSelected.length > 0) {
        Array.prototype.forEach.call(allSelected, (selected) => {
            if ($(selected).attr('name') !== 'catExpBox') {
                if (
                    selected.value &&
                    selectedItems.includes(selected.value) === false
                ) {
                    selectedItems.push(selected.value);
                }
                // if (selected.value && selected.value !== 'head' && selectedItems.includes(selected.value) === false) {
                //     selectedItems.push(selected.value);
            }
        });
    }
    return selectedItems;
};

// PythonのNumpyのhistgram関数相当のヒストグラム計算処理を実施
function genHistLabelsCounts(
    arrayVals,
    localNumBins,
    localValMin,
    localValMax,
) {
    const labelMin = !isEmpty(localValMin)
        ? localValMin
        : Math.min.apply(null, arrayVals);
    const labelMax = !isEmpty(localValMax)
        ? localValMax
        : Math.max.apply(null, arrayVals);

    const histLabels = genHistLabels(labelMin, labelMax, localNumBins);
    const histCounts = genHistCounts(histLabels, arrayVals, labelMin, labelMax);

    // histLablesの配列の最後の要素はhistCountsより1つ多く余分のため削除
    histLabels.pop();
    // histLabelsがヒストグラムのX軸、histCountsがヒストグラムのY軸
    // 表示を合わせるために値を逆順にする
    return {
        histLabels: histLabels.reverse(),
        histCounts: histCounts.reverse(),
    };
}

function genHistLabels(labelMin, labelMax, localNumBins) {
    const histLabels = [];
    const interval = (labelMax - labelMin) / localNumBins;
    let val = labelMin;
    histLabels.push(val);
    for (let i = 0; i < localNumBins; i++) {
        val += interval;
        histLabels.push(Number(val).toPrecision(4));
    }
    return histLabels;
}

function genHistCounts(histLabels, arrayVals, labelMin, labelMax) {
    // まずはhistLabels - 1個分の値0の配列を生成
    const histCounts = [];
    for (let i = 0; i < histLabels.length - 1; i++) {
        histCounts.push(0);
    }
    let numLabelMin = 0;
    let numLabelMax = 0;

    for (let i = 0; i < arrayVals.length; i++) {
        const val = arrayVals[i];
        // 境界条件の不具合を避けるため最小値、最大値は一旦スキップ
        if (val === labelMin) {
            numLabelMin += 1;
            continue;
        }
        if (val === labelMax) {
            numLabelMax += 1;
            continue;
        }
        for (let j = 0; j < histLabels.length; j++) {
            if (j === histLabels.length - 1) {
                break;
            }
            const histLabelMin = histLabels[j];
            const histLabelMax = histLabels[j + 1];
            if (
                val !== null &&
                val !== undefined &&
                val >= histLabelMin &&
                val < histLabelMax
            ) {
                histCounts[j] += 1;
            }
        }
    }
    // 最小値分を一番左の要素に追加
    histCounts[0] += numLabelMin;
    // 最大値分を一番右の要素に追加
    histCounts[histLabels.length - 2] += numLabelMax;
    return histCounts;
}

const endProcMultiSelectOnChange = async (count, props) => {
    const selectedProc = $(`#end-proc-process-${count}`);
    if (selectedProc.length === 0) {
        return;
    }
    const procId = selectedProc[0].value;
    const procInfo = procConfigs[procId];

    $(`#count-variables-${count}`).text(0);

    // remove old elements
    $(`#end-proc-val-${count}`).remove();
    if (procInfo == null) {
        updateSelectedItems();
        return;
    }
    const ids = [];
    const vals = [];
    const names = [];
    const checkedIds = [];
    const dataTypes = [];
    const rawDataTypes = [];
    const columnTypes = [];
    const dataTypeShownName = [];
    await procInfo.updateColumns();
    let procColumns = procInfo.getColumns();

    // const dataTypeTargets = props.showStrColumn ? CfgProcess_CONST.ALL_TYPES : CfgProcess_CONST.NUMERIC_TYPES;

    // push cycle time columns first
    // if (props.showStrColumn && !props.hideCTCol) {
    //     const [ctCol] = procInfo.getCTColumn();
    //     const datetimeCols = procInfo.getDatetimeColumns();
    //     if (ctCol) {
    //         ids.push(ctCol.id);
    //         vals.push(ctCol.name_en);
    //         names.push(ctCol.shown_name);
    //         dataTypes.push(ctCol.data_type);
    //         dataTypeShownName.push(dataTypeShort(ctCol))
    //         rawDataTypes.push(ctCol.raw_data_type);
    //         columnTypes.push(ctCol.column_type);
    //     }
    //       if (datetimeCols) {
    //         for (const dtCol of datetimeCols) {
    //             ids.push(dtCol.id);
    //             vals.push(dtCol.name_en);
    //             names.push(dtCol.shown_name);
    //             dataTypes.push(dtCol.data_type);
    //             dataTypeShownName.push(dataTypeShort(dtCol))
    //             rawDataTypes.push(ctCol.raw_data_type);
    //             rawDataTypes.push(ctCol.column_type);
    //         }
    //     }
    // }

    // ↓--- New logic order columns base on data type & master ---↓
    let getDateColID = null;
    const dataTypeOrder = [
        DataTypes.DATETIME,
        DataTypes.SERIAL,
        DataTypes.JUDGE,
        DataTypes.MASTER,
        DataTypes.CATEGORY,
        DataTypes.REAL,
        DataTypes.SMALL_INT,
        DataTypes.INTEGER,
        DataTypes.BIG_INT,
        DataTypes.STRING,
        DataTypes.BOOLEAN,
    ];
    const masterInnerOrder = [
        masterDataGroup.FACTORY,
        masterDataGroup.PLANT,
        masterDataGroup.PROD_FAMILY,
        masterDataGroup.LINE,
        masterDataGroup.DEPT,
        masterDataGroup.SECT,
        masterDataGroup.PRODUCT,
        masterDataGroup.PARTTYPE,
        masterDataGroup.PART,
        masterDataGroup.EQUIP,
        masterDataGroup.STATION,
    ];

    const getDataType = (column) => {
        if (isMasterDataType(column.column_type)) {
            return DataTypes.MASTER;
        } else if (column.is_get_date) {
            return DataTypes.DATETIME;
        } else if (column.is_serial_no) {
            return DataTypes.SERIAL;
        } else {
            return getDataTypeByRawDataType(column);
        }
    };

    procColumns = procColumns.sort(function (a, b) {
        const a_type = getDataType(a);
        const b_type = getDataType(b);
        const i = dataTypeOrder.indexOf(a_type) - dataTypeOrder.indexOf(b_type);

        if (i < 0) return -1;
        else if (i === 0) {
            if (
                a_type === DataTypes.MASTER.label &&
                b_type === DataTypes.MASTER.label
            ) {
                const ii =
                    masterInnerOrder.indexOf(a.column_type) -
                    masterInnerOrder.indexOf(b.column_type);
                if (ii < 0) return -1;
                else if (ii === 0) return 0;
                else return 1;
            } else {
                return 0;
            }
        } else return 1;
    });
    // ↑--- New logic order columns base on data type & master ---↑

    const [ctCol] = procInfo.getCTColumn();

    for (const col of procColumns) {
        if (col.is_get_date) {
            getDateColID = col.id;
        }

        if (
            props.hideCTCol &&
            CfgProcess_CONST.CT_TYPES.includes(col.data_type)
        ) {
            continue;
        }

        ids.push(col.id);
        vals.push(col.name_en);
        names.push(col.shown_name);
        dataTypes.push(col.data_type);
        dataTypeShownName.push(dataTypeShort(col));
        rawDataTypes.push(col.raw_data_type);
        columnTypes.push(col.column_type);
    }

    // load machine multi checkbox to Condition Proc.
    if (ids) {
        const parentId = `end-proc-val-div-${count}`;
        const availableColorVars = procColumns.filter((col) =>
            [
                DataTypes.STRING.name,
                DataTypes.INTEGER.name,
                DataTypes.TEXT.name,
                DataTypes.DATE.name,
                DataTypes.TIME.name,
            ].includes(col.data_type),
        );
        const listGroupProps = {
            checkedIds,
            name: `GET02_VALS_SELECT${count}`,
            noFilter: false,
            itemNames: names,
            itemDataTypes: props.showDataType ? dataTypes : null,
            itemRawDataTypes: props.showDataType ? rawDataTypes : null,
            itemColumnTypes: props.showDataType ? columnTypes : null,
            itemDataTypeShownNames: props.showDataType
                ? dataTypeShownName
                : null,
            isRadio: !!props.radio,
            showCatExp: props.showCatExp,
            isRequired: props.isRequired,
            getDateColID,
            showObjectiveInput: props.showObjective,
            showLabel: props.showLabel,
            groupIDx: count,
            showColor: props.showColor,
            hasDiv: props.hasDiv,
            hideStrVariable: props.hideStrVariable,
            hideRealVariable: props.hideRealVariable,
            colorAsDropdown: props.colorAsDropdown,
            availableColorVars,
            optionalObjective: props.optionalObjective,
            objectiveHoverMsg: props.objectiveHoverMsg,
            showFilter: props.showFilter,
            allowObjectiveForRealOnly: props.allowObjectiveForRealOnly || false,
            judge: props.judge || false,
            shouldObjectiveIsTarget: props.shouldObjectiveIsTarget || false,
            disableSerialAsObjective: props.disableSerialAsObjective || false,
            procId,
        };
        addGroupListCheckboxWithSearch(
            parentId,
            `end-proc-val-${count}`,
            '',
            ids,
            vals,
            listGroupProps,
        );
    }
    updateSelectedItems();
    onchangeRequiredInput();
    setProcessID();
    setColorRelativeStartEndProc();
    checkIfProcessesAreLinked();
};

// add end proc
const addEndProcMultiSelect = (procIds, procVals, props) => {
    let count = 1;
    const innerFunc = (
        onChangeCallbackFunc = null,
        onCloseCallbackFunc = null,
        onChangeCallbackDicParam = null,
        onCloseCallbackDicParam = null,
    ) => {
        const itemList = [];
        for (let i = 0; i < procIds.length; i++) {
            const itemId = procIds[i];
            const itemVal = procVals[i].shown_name;
            const itemEnVal = procVals[i].name_en;
            itemList.push(
                `<option value="${itemId}" title="${itemEnVal}">${itemVal}</option>`,
            );
        }

        while (checkExistDataGenBtn('btn-add-end-proc', count)) {
            count = countUp(count);
        }

        const parentID = `end-proc-process-div-${count}-parent`;

        const proc = `<div class="col-12 col-xl-6 col-lg-12 col-md-12 col-sm-12 p-1">
                <div class="card end-proc dynamic-element table-bordered py-sm-3" id="${parentID}">
                        <span class="pull-right clickable close-icon" data-effect="fadeOut">
                            <i class="fa fa-times"></i>
                        </span>
                        <div class="d-flex align-items-center" id="end-proc-process-div-${count}">
                            <span class="mr-2">${i18nCommon.process}</span>
                            <div class="w-auto flex-grow-1 position-relative">
                                <i id="no-link-with-start-proc-${count}" class="fas fa-triangle-exclamation position-absolute blink" style="top: 10px; right: 60px; z-index: 1; display: none; color:yellow;"></i>
                                <span class="position-absolute count-variable-label" style="top: 7px; right: 40px; z-index: 1;" id="count-variables-${count}">0</span>
                                <select class="form-control select2-selection--single
                                    ${props.isRequired ? 'required-input' : ''}
                                    select-n-columns" name="end_proc${count}"
                                    id="end-proc-process-${count}" 
                                    data-gen-btn="btn-add-end-proc">
                                    ${itemList.join(' ')}
                                </select>
                            </div>
                        </div>
                        <div id="end-proc-val-div-${count}">
                        </div>
                     </div>
                    </div>`;

        $('#end-proc-row div').last().before(proc);
        $(`#end-proc-process-${count}`).on('change', (e) => {
            const eleNumber = e.currentTarget.id.match(/\d+$/)[0];
            const isShowCTTime = $(formElements.showCT_Time).prop('checked');
            if (isShowCTTime !== undefined && props.hideCTCol !== undefined) {
                props.hideCTCol = isShowCTTime ? !isShowCTTime : true;
            }
            endProcMultiSelectOnChange(eleNumber, props).then((r) => {
                if (onChangeCallbackFunc) {
                    if (onChangeCallbackDicParam) {
                        onChangeCallbackFunc(onChangeCallbackDicParam);
                    } else {
                        onChangeCallbackFunc(e);
                    }
                }
            });
            countTotalVariables();
        });

        cardRemovalByClick(
            '#end-proc-row div',
            onCloseCallbackFunc,
            onCloseCallbackDicParam,
        );
        updateSelectedItems();
    };
    return innerFunc;
};

const getStratifiedVars = async (selectedEndProc) => {
    if (!selectedEndProc) {
        return;
    }
    const svcolumns = $(eles.sVColumns);
    svcolumns.html('');

    const procInfo = procConfigs[selectedEndProc];
    await procInfo.updateColumns();
    const stratifiedVarColumns = procInfo.getCategoryColumns();

    let maxLen = 0;
    for (const col of stratifiedVarColumns) {
        if (col.shown_name && col.shown_name.length > maxLen) {
            maxLen = col.shown_name.length;
        }
    }

    let stratifiedVarOptions = '<option value="">---</option>';
    stratifiedVarColumns.forEach((col) => {
        stratifiedVarOptions += `<option value="${col.id}" title="${col.name_en}">${col.shown_name}</option>`;
    });
    svcolumns.html(stratifiedVarOptions);
};

const afterRequestAction = () => {
    loadingHide();
    scrollList2Top();
};

const scrollList2Top = () => {
    $('.list-group').scrollTop(0);
    $('ul').scrollTop(0);
};

const updateTableRowNumber = (
    tableId = null,
    tableElement = null,
    tableBody = null,
) => {
    // numbering rows in a table
    try {
        if (!isEmpty(tableElement)) {
            tableElement.find('tbody tr').each((rowIdx, row) => {
                $(row)
                    .find('td.col-number')
                    .text(rowIdx + 1);
            });
            return;
        }
        if (!isEmpty(tableId)) {
            $(`#${tableId} tbody tr`).each((rowIdx, row) => {
                $(row)
                    .find('td.col-number')
                    .text(rowIdx + 1);
            });
            return;
        }

        if (!isEmpty(tableBody)) {
            $(tableBody)
                .find('tr')
                .each((rowIdx, row) => {
                    $(row)
                        .find('td.col-number')
                        .text(rowIdx + 1);
                });
        }
    } catch (e) {
        console.error(e);
        //
    }
};

const collapseFloatingList = (el) => {
    // collapse <ul> list
    $(el).removeClass('floating-dropdown');
    $(el).removeClass('disable-max-height');
    $(el).css({ width: '', height: '', transform: '' });
    $(el).parent().css({ height: '' });

    // const shouldReOrder = (el) => {
    //     const options = $(el).find('li input[type=checkbox], li input[type=radio]');
    //     for (let i = 0; i < options.length; i++) {
    //         if (i > 0) {
    //             const isChecked = options[i].checked;
    //             const isPreviousChecked = options[i - 1].checked;
    //             const previousVal = options[i - 1].value;
    //             const selectedButNotTop = i > 1 && isChecked && !isPreviousChecked && previousVal !== 'All';
    //             if (selectedButNotTop) {
    //                 return true;
    //             }
    //         }
    //     }
    //
    //     return false;
    // };

    // check before re-order
    // if (!shouldReOrder(el)) {
    //     return;
    // }

    // move selected options to top
    const isChanged = sortHtmlElements($(el));
    if (isChanged) {
        $(el).scrollTop(0);
    }
};
const collapseFloatingLists = () => {
    $('.floating-dropdown').each((i, el) => {
        collapseFloatingList(el);
    });
};

let savedBox = null;
const keyPress = (e) => {
    if (e.key === 'Escape') {
        collapseFloatingLists();
    } else if (e.key === 'ArrowLeft') {
        if (savedBox) {
            // TODO use common function, doesn't work now
            $('.cate-tooltip').css({ visibility: 'hidden' }); // hide all tooltip
            let count = 0;
            let previousBox = savedBox.previousSibling;
            while (previousBox && count < 2000) {
                // TODO limit to a max iteration
                if ($(previousBox).hasClass('keyboard-movement')) {
                    // reset previous box
                    $(
                        '#cateTable table td.box-has-data.cate-box-border',
                    ).removeClass('cate-box-border');
                    $(savedBox).removeClass('cate-box-border');

                    // update current box
                    savedBox = previousBox;
                    $(savedBox).addClass('cate-box-border');
                    $(savedBox)
                        .find('span.cate-value .cate-tooltip')
                        .css({ visibility: 'visible' });
                    break;
                } else {
                    previousBox = previousBox.previousSibling;
                }
                count += 1;
            }
        }
    } else if (e.key === 'ArrowRight') {
        if (savedBox) {
            $('.cate-tooltip').css({ visibility: 'hidden' });
            let count = 0;
            let nextBox = savedBox.nextSibling;
            while (nextBox && count < 2000) {
                if ($(nextBox).hasClass('keyboard-movement')) {
                    $(
                        '#cateTable table td.box-has-data.cate-box-border',
                    ).removeClass('cate-box-border');
                    $(savedBox).removeClass('cate-box-border');

                    savedBox = nextBox;
                    $(savedBox).addClass('cate-box-border');
                    $(savedBox)
                        .find('span.cate-value .cate-tooltip')
                        .css({ visibility: 'visible' });
                    break;
                } else {
                    nextBox = nextBox.nextSibling;
                }
                count += 1;
            }
        }
    }
};

/**
 * Decorator function to write log
 * @param {function(...[*]): *} func - A function that want to be wrapped
 * @param {string?} prefix - A prefix string that add to log
 * @param {boolean?} isLog - A function that want to be wrapped
 * @return {function(...[*]): *} - a function
 */
function decoratorLog(func, prefix = '', isLog = true) {
    if (!isLog) {
        return func;
    }

    return function (...args) {
        const start = performance.now();
        consoleLogDebug(`${prefix}[Start] Function: ${func.name}`);
        try {
            return func.call(this, ...args);
        } finally {
            const duration = performance.now() - start;
            consoleLogDebug(
                `${prefix}[End] Function: ${func.name}; Execution time: ${duration} milliseconds`,
            );
        }
    };
}

const loadingShow = (
    isContinue = false,
    showGraph = false,
    autoHideAfterSecond = null,
) => {
    const resetProgress = () => {
        // console.log('resetProgress');
        loadingProgressBackend = 0;
    };

    // init
    setTimeout(() => {
        loadingShowImmediately(showGraph);

        if (showGraph) {
            $('.loadingoverlay_fa').addClass('position-absolute');
            const elapsedTimeEl = $('#show-elapsed-time');
            displayElapsedTime(elapsedTimeEl);
            showElapsedTime = setInterval(() => {
                if (!requestStartedAt) {
                    clearInterval(showElapsedTime);
                    return;
                }
                displayElapsedTime(elapsedTimeEl);
            }, 1000);
        }
    }, 0);

    const displayElapsedTime = (elapsedTimeEl) => {
        if (!requestStartedAt) return;

        const t1 = performance.now();
        const elapsedTime = t1 - requestStartedAt;
        const h = Math.floor(elapsedTime / (1000 * 60 * 60));
        const m = Math.floor(elapsedTime / (1000 * 60)) - h * 60;
        const s = Math.round(elapsedTime / 1000) - m * 60;
        const time = `${h > 0 ? addZeroToNumber(h) + ':' : ''}${addZeroToNumber(m)}:${addZeroToNumber(s)}`;
        elapsedTimeEl.text(`Elapsed time: ${time}`);
    };

    if (!isContinue) {
        resetProgress();
    }

    if (autoHideAfterSecond) {
        // Auto hide loading after {autoHideAfterSecond} seconds
        setTimeout(function () {
            if (isLoadingShow()) {
                loadingHide();
            }
        }, autoHideAfterSecond);
    }
};

/**
 * Show loading screen
 * @param {boolean} showGraph - is mode for show graph page or not
 */
const loadingShowImmediately = (showGraph = false) => {
    const abortButtonHtml = `
        <div class='abort-button-div'>
            <button class='btn btn-sm btn-danger abort-button' onclick='handleShowAbortModal()'>
                <i class='fa fa-times mr-2'></i>
                <span>ABORT</span>
            </button>
            <span id="show-elapsed-time"></span>
        </div>
    `;

    $.LoadingOverlay('show', {
        image: '',
        progress: true,
        progressFixedPosition: 'top',
        progressColor: 'rgba(170, 170, 170, 1)',
        size: showGraph ? 20 : 8,
        maxSize: 0,
        background: 'rgba(170, 170, 170, .25)',
        fontawesomeColor: 'rgba(170, 170, 170, 1)',
        fontawesome: 'fa fa-spinner fa-spin',
        fontawesomeResizeFactor: 2,
        custom: showGraph ? $(abortButtonHtml) : null,
    });
};

const sleep = (second) =>
    new Promise((resolve) => setTimeout(resolve, second * 1000));

const removeAbortButton = async (res) => {
    $('.abort-button').prop('disabled', true);
    clearInterval(showElapsedTime);
    $('#confirmAbortProcessModal').modal('hide');
    afterReceiveResponseCommon(res);
    await sleep(0.3);
};

const loadingUpdate = (pct) => {
    setTimeout(() => {
        // console.log('loadingUpdate', parseInt(pct));
        $.LoadingOverlay('progress', parseInt(pct));
    }, 0);
};

const loadingHide = () => {
    setTimeout(() => {
        // console.log('loadingHide');
        $.LoadingOverlay('progress', 100);
        $.LoadingOverlay('hide', true);
    }, 0);
};

const isLoadingShow = () => $('.loadingoverlay').length > 0;

const loadingHideDelayTime = (noRecords) =>
    // console.log('loadingHideDelayTime', Math.min((noRecords || 1000), 1000)/3);
    Math.min(noRecords || 1000, 1000) / 3;

const errorHandling = (error, type = '') => {
    if (error.statusText === 'abort') return;

    const timeout = error.statusText || error.message;
    if (timeout.toLowerCase().includes('timeout')) {
        abortProcess();
        console.log('request timeout..');
        const i18nTexts = {
            abnormalGraphShow:
                type === 'front'
                    ? $('#i18nFrontProcessTimeout').text()
                    : $('#i18nRequestTimeout').text(),
        };

        const msgContent = `<p>${i18nTexts.abnormalGraphShow}</p>`;

        showToastrMsg(msgContent, MESSAGE_LEVEL.ERROR);
    } else {
        showToastrAnomalGraph();
    }
};

const handleShowAbortModal = () => {
    $('#confirmAbortProcessModal').modal('show');
    $('#confirmAbortProcessModal').css({ zIndex: 2147483649 });
    $('.loadingoverlay').css({ zIndex: 9999 });
};

const abortProcess = () => {
    if (xhr) {
        ajaxWithLog({
            url: '/ap/api/common/abort_process',
            method: 'GET',
            data: {
                thread_id: xhr.thread_id,
            },
        });
        xhr.abort();
        xhr = null;
    }
};

class GraphStore {
    constructor() {
        this.dctCanvas2TimeSeries = {};
        this.dctCanvas2Whisker = {};
        this.dctCanvas2Histogram = {};
        this.dctCanvas2Scatter = {};

        // map: histogram -> timeseries of the same row
        this.dctHistId2TimesSeries = {};

        // trace data output
        this.traceDataResult = {};
        this.dataPointCount = 0;

        // store clicked data points of time series chart
        this.clickedPointIndexes = new Set();

        // canvasId -> hovered index + datasetIndex; use canvasId to get chart object later on
        // 1 chart can have many datasets -> save datasetIndex
        this.lastHoveredDataPoint = {};

        this.selectedCanvas = null;
    }

    setSelectedCanvas(canvasId) {
        this.selectedCanvas = canvasId;
    }

    getSelectedCanvas() {
        return this.selectedCanvas;
    }

    saveHoveredDataPoint(canvasId, hoveredIndex) {
        this.lastHoveredDataPoint[canvasId] = hoveredIndex;
    }

    getLastHoveredDataPoint(canvasId) {
        return this.lastHoveredDataPoint[canvasId];
    }

    setClickedPointIndexes(indexes) {
        this.clickedPointIndexes = indexes;
    }

    getClickedPointIndexes() {
        return this.clickedPointIndexes;
    }

    addClickedPointIndexes(index) {
        this.clickedPointIndexes.add(index);
    }

    getAllTimeSeries() {
        return Object.values(this.dctCanvas2TimeSeries) || [];
    }

    getAllTimeSeriesCanvasIds() {
        return Object.keys(this.dctCanvas2TimeSeries) || [];
    }

    getAllHistogram() {
        return Object.values(this.dctCanvas2Histogram) || [];
    }

    getAllScatterPlot() {
        return Object.values(this.dctCanvas2Scatter) || [];
    }

    setDctCanvas2Scatter(dictCanvas2Scatter) {
        this.dctCanvas2Scatter = dictCanvas2Scatter;
    }

    addTimeSeriesObj(canvasId, graphObj) {
        this.dctCanvas2TimeSeries[canvasId] = graphObj;
    }

    addWhiskerObj(canvasId, graphObj) {
        this.dctCanvas2Whisker[canvasId] = graphObj;
    }

    addHistogramObj(canvasId, graphObj) {
        this.dctCanvas2Histogram[canvasId] = graphObj;
    }

    addScatterObj(canvasId, graphObj) {
        this.dctCanvas2Scatter[canvasId] = graphObj;
    }

    addHist2TimeSeries(canvasId, tsGraphObj) {
        this.dctHistId2TimesSeries[canvasId] = tsGraphObj;
    }

    getTimeSeriesById(canvasId) {
        return this.dctCanvas2TimeSeries[canvasId];
    }

    getWhiskerById(canvasId) {
        return this.dctCanvas2Whisker[canvasId];
    }

    getHistById(canvasId) {
        return this.dctCanvas2Histogram[canvasId];
    }

    getScatterById(canvasId) {
        return this.dctCanvas2Scatter[canvasId];
    }

    getTimeSeriesFromHist(canvasId) {
        return this.dctHistId2TimesSeries[canvasId];
    }

    destroyAllGraphInstances() {
        this.dctCanvas2TimeSeries = {};
        this.dctCanvas2Histogram = {};
        this.dctCanvas2Scatter = {};
        this.dctHistId2TimesSeries = {};

        // destroy all old chart instances before showing new graphs
        for (const graphIdx in Chart.instances) {
            try {
                Chart.instances[graphIdx].destroy();
            } catch (error) {
                console.log(error);
            }
        }
    }

    setTraceData(data) {
        this.traceDataResult = data;
        this.countDataPoint();
    }

    getTraceData() {
        return this.traceDataResult;
    }

    updateEndProc(endProc) {
        this.traceDataResult.COMMON.end_proc = endProc;
    }

    getDataAtIndex(dataIdx, dataPointIdx) {
        const plotDatas =
            getNode(this.traceDataResult, ['array_plotdata']) || [];
        const empty = { x: '', y: '' };
        if (isEmpty(plotDatas) || plotDatas.length <= dataIdx) {
            return empty;
        }
        const plotData = plotDatas[dataIdx];
        if (isEmpty(plotData) || plotData.length <= dataPointIdx) {
            return empty;
        }
        return {
            x: plotData.array_x[dataPointIdx],
            y: plotData.array_y[dataPointIdx],
        };
    }

    getArrayPlotData(canvasId) {
        const dataIdx = $(`#${canvasId}`).attr('plotdata-index') || 0;
        const plotDatas =
            getNode(this.traceDataResult, ['array_plotdata']) || [];
        if (isEmpty(plotDatas) || plotDatas.length <= dataIdx) {
            return empty;
        }
        return plotDatas[dataIdx];
    }

    countDataPoint() {
        if (isEmpty(this.traceDataResult)) {
            this.dataPointCount = 0;
        } else {
            const timeColLenght =
                'times' in this.traceDataResult
                    ? this.traceDataResult.times.length
                    : 0;
            if (timeColLenght) {
                const numSensors = this.traceDataResult.array_plotdata.length;
                this.dataPointCount = (timeColLenght + 128) * numSensors * 2;
            }
        }
    }

    getCountDataPoint() {
        console.log('this.dataPointCount: ', this.dataPointCount);
        return this.dataPointCount || 0;
    }

    getPCPArrayPlotDataByID(colID) {
        const plotDatas =
            getNode(this.traceDataResult, ['array_plotdata']) || [];
        if (isEmpty(plotDatas)) {
            return {};
        }
        const plotObj = {};
        plotDatas.forEach((plot, k) => {
            if (plot.col_detail && plot.col_detail.id === colID) {
                plotObj.key = k;
                plotObj.data = plot;
            }
        });
        return plotObj;
    }

    getMasterInfo(canvasId, dataPoint) {
        const currentTraceData = graphStore.getTraceData();
        const dataIndex = $(`#${canvasId}`).attr('plotdata-index') || 0;
        const isMasterCol =
            currentTraceData.array_plotdata[dataIndex].is_master_col;
        if (!isMasterCol) {
            return null;
        }
        const rankColId =
            currentTraceData.array_plotdata[dataIndex].array_y[
                dataPoint.dataIndex
            ];
        const masterId =
            currentTraceData.array_plotdata[dataIndex].rank_cols_to_master_ids[
                rankColId
            ];
        const masterName =
            currentTraceData.array_plotdata[dataIndex].end_col_name ?? '';
        const masterInfo = getMasterInfoHovering(masterId, masterName);
        return masterInfo;
    }

    getVariableOrdering(
        procConfig,
        useFeatureImportance = true,
        loadByOrderIDs = false,
    ) {
        const ordering = [];
        let orderingID = [];
        let allColIds = [];
        const currentTrace = this.traceDataResult;
        const objectiveVariable = currentTrace.COMMON.objectiveVar
            ? Number(currentTrace.COMMON.objectiveVar[0])
            : undefined;
        const hasObjectiveVarInGUI = $(
            'input[name=objectiveVar]:checked',
        ).length;
        // const latestSortProcs = latestSortColIds.map(val => Number(val.split('-')[0]));
        // const endProcIds = latestSortProcs.filter((procId, index, procs) => procs.indexOf(procId) === index);
        const endProcIds = getSelectedEndProcIds();
        let endProcColIds = endProcIds
            .map((id) => procConfig[id].getColumns())
            .flat();
        const endProcObj = Object.fromEntries(
            endProcColIds.map((proc) => [proc.id, proc.process_id]),
        );

        if (objectiveVariable && hasObjectiveVarInGUI) {
            orderingID.push(objectiveVariable);
        }

        let sensorList = [];
        if (
            useFeatureImportance &&
            currentTrace.importance_columns_ids &&
            currentTrace.importance_columns_ids.length
        ) {
            sensorList = currentTrace.importance_columns_ids;
            endProcColIds = [];
        }
        if (
            currentTrace.COMMON.GET02_VALS_SELECT &&
            currentTrace.COMMON.GET02_VALS_SELECT.length &&
            !useFeatureImportance
        ) {
            sensorList = currentTrace.COMMON.GET02_VALS_SELECT;
        }
        if (loadByOrderIDs) {
            sensorList = latestSortColIds.map((val) =>
                Number(val.split('-')[1]),
            );
        }
        orderingID = [...orderingID, ...sensorList];
        allColIds = [...orderingID, ...endProcColIds.map((col) => col.id)];
        allColIds = allColIds.filter(
            (value, index, array) => array.indexOf(value) === index,
        );
        allColIds = new Set(allColIds);
        allColIds = Array.from(allColIds);
        if (allColIds.length) {
            allColIds.forEach((id) => {
                // const targetProcID = currentTrace.COMMON.end_proc[id];
                const targetProcID = endProcObj[id];
                const targetProc = procConfig[targetProcID];
                const targetCol = targetProc && targetProc.dicColumns[id];
                if (targetCol) {
                    ordering.push({
                        ...targetCol,
                        id: id,
                        name: targetCol.shown_name,
                        proc_name: targetProc.shown_name,
                        type: targetCol.data_type,
                        proc_id: targetProc.id,
                        bridge_column_type: targetCol.column_type,
                    });
                }
            });
        }
        return {
            ordering,
            objectiveVar: objectiveVariable,
            orderingID: orderingID,
            use_feature_importance: true,
        };
    }
    genDfLabel(
        id,
        name,
        dfMode,
        mode,
        isNGRate = false,
        needToMultipleID = false,
    ) {
        let labelID = id;
        let modeLabel = mode.charAt(0).toUpperCase() + mode.slice(1);
        modeLabel = `EMD|${modeLabel}`;
        if (isNGRate) {
            labelID = Number(id) * RLP_DUMMY_ID;
            modeLabel = 'NG Rate';
        } else if (needToMultipleID && mode == EMDType.DIFF) {
            labelID = Number(id) * EMD_DIFF_ID;
        }
        const colName = dfMode ? `${name}|${modeLabel}` : name;
        return [labelID, colName, `__${labelID}__${name}|${modeLabel}`];
    }
    getTargetVariables(dfMode = true) {
        const currentTrace = this.traceDataResult;
        const targetVariables = [];
        const RLPMode = currentTrace.emdType;
        // ng rate append
        if (
            dfMode &&
            COMMON_CONSTANT.NG_RATE in currentTrace &&
            currentTrace.ng_rates
        ) {
            currentTrace.ng_rates.forEach((sensorDat) => {
                const [colID, colName, colLabel] = this.genDfLabel(
                    sensorDat.end_col_id,
                    sensorDat.sensor_name,
                    dfMode,
                    RLPMode,
                    true,
                );
                targetVariables.push({
                    id: colID,
                    org_id: sensorDat.end_col_id,
                    name: colName,
                    proc_id: sensorDat.end_proc_id,
                    proc_name: sensorDat.end_proc_name,
                    type: sensorDat.data_type.toUpperCase(),
                    data_type: sensorDat.data_type.toUpperCase(),
                    label: colLabel,
                });
            });
        }
        // end variable append
        currentTrace.array_plotdata.forEach((sensorDat) => {
            const sensor = procConfigs[sensorDat.end_proc_id].getColumnById(
                sensorDat.sensor_id,
            );
            if ([EMDType.BOTH, EMDType.DRIFT].includes(RLPMode)) {
                const [colID, colName, colLabel] = this.genDfLabel(
                    sensorDat.sensor_id,
                    sensorDat.sensor_name,
                    dfMode,
                    EMDType.DRIFT,
                );
                targetVariables.push({
                    id: colID,
                    org_id: sensorDat.sensor_id,
                    name: colName,
                    proc_id: sensorDat.end_proc_id,
                    proc_name: sensorDat.proc_name,
                    type: sensor.data_type,
                    data_type: sensor.data_type,
                    label: colLabel,
                });
            }
            if (dfMode && [EMDType.BOTH, EMDType.DIFF].includes(RLPMode)) {
                const needToMultipleID = RLPMode == EMDType.BOTH;
                const [colID, colName, colLabel] = this.genDfLabel(
                    sensorDat.sensor_id,
                    sensorDat.sensor_name,
                    dfMode,
                    EMDType.DIFF,
                    false,
                    needToMultipleID,
                );
                targetVariables.push({
                    id: colID,
                    org_id: sensorDat.sensor_id,
                    name: colName,
                    proc_id: sensorDat.end_proc_id,
                    proc_name: sensorDat.proc_name,
                    type: sensor.data_type,
                    data_type: sensor.data_type,
                    label: colLabel,
                });
            }
        });
        return targetVariables;
    }
}

const significantDigitFmt = (val, sigDigit = 4) => {
    if (!val || typeof val !== 'number') return '';
    let fmt = '';
    if (sigDigit < 1) {
        sigDigit = 1;
    } else if (sigDigit > 6) {
        sigDigit = 6;
    }

    const digit = Math.floor(Math.log10(Math.abs(val)));
    if (Number.isInteger(val)) {
        if (val / 100000000 > 1) {
            fmt = '';
        } else {
            fmt = ',d';
        }
    } else if (digit < -3 || digit > 6) {
        fmt = `.${sigDigit - 1}e`;
    } else if (digit > sigDigit - 3) {
        fmt = ',.1f';
    } else {
        fmt = `,.${sigDigit - digit - 1}f`;
    }

    return fmt;
};

const applySignificantDigit = (val, sigDigit = 4, fmtStr = '') => {
    try {
        if (typeof val === 'string' || val == null) return val;
        const fmt = fmtStr ? fmtStr : significantDigitFmt(val, sigDigit);
        if (!fmt) return val.toString();
        if (fmt) {
            const d3FormattedValue = d3.format(fmt)(val);
            const stringValue = d3FormattedValue.toString();
            // fix for case: 1.123456789e30 -> 1.1234e30

            if (!stringValue.includes('e')) return stringValue;

            const splittedNum = stringValue.split('.');
            let returnValue = splittedNum[0];
            if (splittedNum.length > 1) {
                const decimalNum = splittedNum[1].split('e');
                if (decimalNum[0].length > sigDigit) {
                    returnValue += '.';
                    returnValue += decimalNum[0].slice(0, sigDigit);
                    if (decimalNum.length > 1) {
                        returnValue += 'e';
                        returnValue += decimalNum[1];
                    }
                    return returnValue;
                }
            }

            return stringValue;
        }
    } catch (e) {
        return significantDigitFmt(val, sigDigit)
            ? applySignificantDigit(val, sigDigit)
            : val.toString();
    }
};

const makeDictFrom2Arrays = (keys, vals) => {
    const result = {};
    keys.forEach((key, i) => (result[key] = vals[i]));

    return result;
};

// draw processing time
const drawProcessingTime = (
    t0,
    t1,
    backendTime,
    rowNumber,
    uniqueSerial = null,
) => {
    const frontendTime = (t1 - t0) / 1000;
    const netTime = (t1 - (requestStartedAt || t0)) / 1000; // seconds
    const hasDuplicate = $('[name=duplicated_serial]').length > 0;
    const duplicate = hasDuplicate
        ? `, Duplicate: ${uniqueSerial !== null ? applySignificantDigit(rowNumber - uniqueSerial) : 'No checked'}`
        : '';
    let numberOfQueriedDat = checkTrue(rowNumber)
        ? `, Number of queried data : ${applySignificantDigit(rowNumber)}${
              duplicate
          }`
        : '';
    let processTime = `Net time: ${applySignificantDigit(netTime)} sec, Tb: ${
        backendTime ? applySignificantDigit(backendTime) : 0
    } sec, Tf: ${applySignificantDigit(frontendTime)} sec${numberOfQueriedDat}`;

    const lastUpdateTime = `Last update time: ${moment().format(DATETIME_FORMAT)}`;
    $('#lastUpdateTime').html(lastUpdateTime);
    $('#processingTime').html(processTime);

    // reset start time
    requestStartedAt = null;
    // common call backend to update exe time
    ajaxWithLog({
        url: '/ap/api/common/draw_plot_excuted_time',
        data: {
            dataset_id: dataSetID,
            executed_time: Math.round(frontendTime),
        },
    });
};

const getScaleInfo = (plotdata, scaleOption) => {
    switch (scaleOption) {
        case scaleOptionConst.SETTING:
            return plotdata.scale_setting;
        case scaleOptionConst.COMMON:
            return plotdata.scale_common;
        case scaleOptionConst.THRESHOLD:
            return plotdata.scale_threshold;
        case scaleOptionConst.AUTO:
            return plotdata.scale_auto;
        case scaleOptionConst.FULL_RANGE:
            return plotdata.scale_full;
    }

    return null;
};

const chartAreaBorder = {
    id: 'chartAreaBorder',
    beforeDraw(chart, args, options) {
        const {
            ctx,
            chartArea: { left, top, width, height },
        } = chart;
        ctx.save();
        ctx.strokeStyle = options.borderColor;
        ctx.lineWidth = options.borderWidth;
        ctx.setLineDash(options.borderDash || []);
        ctx.lineDashOffset = options.borderDashOffset;
        ctx.strokeRect(left, top, width, height);
        ctx.restore();
    },
};

const scatterInsideTitle = {
    id: 'scatterInsideTitle',
    beforeDatasetsDraw(chart, args, options) {
        const {
            ctx,
            chartArea: { left, top, width, height },
        } = chart;
        ctx.save();
        ctx.font = `${options.font.size}px ${options.font.family}`;

        if (options.xContent.length) {
            ctx.fillStyle = options.color[0];
            const xTextWidth = ctx.measureText(options.xContent[0]).width;
            ctx.fillText(
                options.xContent,
                left + (width / 2 - xTextWidth / 2),
                height - top + 10,
            );
        }
        if (options.yContent.length) {
            ctx.fillStyle = options.color[1];
            const yTextWidth = ctx.measureText(options.yContent[0]).width;
            ctx.translate(70, top + (height / 2 + yTextWidth / 2));
            ctx.rotate(-0.5 * Math.PI);
            ctx.fillText(options.yContent, 0, 10);
        }

        ctx.restore();
    },
};

// transform facet params
const transformFacetParams = (formData, eleIdPrefix = '') => {
    const allFacets = formData.getAll('catExpBox');

    // find level1
    if (allFacets.includes(facetLevels.LV_1)) {
        getSetFacetValue(facetLevels.LV_1, 'catExpBox1', formData, eleIdPrefix);
    }
    // find level2
    if (allFacets.includes(facetLevels.LV_2)) {
        getSetFacetValue(facetLevels.LV_2, 'catExpBox2', formData, eleIdPrefix);
    }

    if (allFacets.includes(facetLevels.DIV)) {
        getSetFacetValue(facetLevels.DIV, 'div', formData, eleIdPrefix);
    }
    formData.delete('catExpBox');
    return formData;
};

const getSetFacetValue = (value, name, formData, eleIdPrefix) => {
    const endProcDiv = eleIdPrefix
        ? `${eleIdPrefix}-end-proc-row`
        : 'end-proc-row';
    const facetItem = $(
        `#${endProcDiv} select[name="catExpBox"] option:selected[value="${value}"]`,
    )
        .parent()
        .attr('id');
    if (facetItem) {
        const facetItemObj2 = facetItem.split('-');
        const facetItemID = facetItemObj2[facetItemObj2.length - 1];
        formData.set(name, facetItemID);
    }
};

const collectFormData = (formID) => {
    const form = $(formID);
    const formData = new FormData(form[0]);
    // formData = chooseTraceTimeInterval(formData);
    for (const item of formData.entries()) {
        const key = item[0];
        const value = item[1];
        if (/cond_proc/.test(key) && isEmpty(value)) {
            formData.delete(key);
        }
    }

    return formData;
};

const bindCategoryParams = (formData) => {
    for (const item of formData.entries()) {
        const key = item[0];
        const value = item[1];
        let endProcId = null;
        if (key.includes('GET02_CATE_SELECT') && !isEmpty(value)) {
            const [, procGroupIdx] = key.split('GET02_CATE_SELECT');
            if (procGroupIdx) {
                endProcId = formData.get(`end_proc${procGroupIdx}`);
            }
            if (endProcId) {
                formData.set(`end_proc_cate${procGroupIdx}`, endProcId);
            }
        }
    }
    return formData;
};

const hideTooltip = (selector, timeOut = 1000) => {
    setTimeout(() => {
        $(selector).tooltip('hide').attr('data-original-title', '');
    }, timeOut);
};

const setTooltip = (selector, message, autoHide = true) => {
    if ($(selector).parent().is(':visible')) {
        const title = $(selector).attr('title');
        $(selector)
            .attr('data-original-title', message)
            .attr('title', '')
            .tooltip('show');

        if (autoHide) {
            hideTooltip(selector);
            if (title) {
                $(selector).attr('title', title);
            }
        }
    }
};

// transform KDE value to max value of histogram
const getTransKDE = (kdeData) => {
    const kdes = [...kdeData.kde].reverse();
    const labelReversed = [...kdeData.hist_labels].reverse();
    let histCounts = [...kdeData.hist_counts];
    // sort number label desc
    const hisLabels = [...kdeData.hist_labels].sort((a, b) => b - a);
    if (JSON.stringify(hisLabels) === JSON.stringify(labelReversed)) {
        histCounts = histCounts.reverse();
    }
    const [, maxKDE] = findMinMax(kdes);
    const [, maxHist] = findMinMax(histCounts);
    const transKDE = kdes.map((i) => (maxHist * i) / maxKDE);
    return {
        transKDE,
        hisLabels,
        histCounts,
    };
};

const transformDatetimeRange = (formData) => {
    const datetimeRangeKeys = [
        'DATETIME_RANGE_PICKER',
        CONST.STARTDATE,
        CONST.STARTTIME,
        CONST.ENDDATE,
        CONST.ENDTIME,
    ];
    const validDatetimeRanges = formData
        .getAll(datetimeRangeKeys[0])
        .filter((value) => value !== '');
    if (validDatetimeRanges.length) {
        // to remove empty datetime range from terms
        datetimeRangeKeys.forEach((key) => formData.delete(key));
        validDatetimeRanges.forEach((value) => {
            formData.append('DATETIME_RANGE_PICKER', value);
            const [startDate, startTime, , endDate, endTime] = value.split(' ');
            formData.append(CONST.STARTDATE, startDate);
            formData.append(CONST.STARTTIME, startTime);
            formData.append(CONST.ENDDATE, endDate);
            formData.append(CONST.ENDTIME, endTime);
        });
    }
    // assign start/end datetime
    return formData;
};

const transformCategoryTraceTime = (formData, eleIdPrefix) => {
    const traceTime1 = formData.get(`${eleIdPrefix}TraceTime1`);
    const traceTime = formData.get('traceTime') || null;
    if (!traceTime1 && !traceTime) {
        formData.set(`${eleIdPrefix}TraceTime1`, traceTime);
    }
    return formData;
};

const transformCHMParams = (formData) => {
    // set params
    const catExpVal =
        formData.get('catExpBox1') ||
        formData.get('catExpBox2') ||
        formData.get('catExpBox');
    if (catExpVal) {
        formData.set('categoryVariable1', catExpVal);
        formData.set('categoryValueMulti1', 'NO_FILTER'); // default
    }
    return formData;
};

const transformSKDParam = (formData, procConf) => {
    const objectiveVarID = formData.get('objectiveVar');
    const valsSelected = formData.getAll('GET02_VALS_SELECT1');
    const setEndProcs = () => {
        const endProcs = [...formData.keys()].filter((key) =>
            key.includes('end_proc'),
        );
        if (endProcs.length) {
            endProcs.forEach((key) => {
                const [, procCountId] = key.split('end_proc');
                formData.set(`end_proc_cate${procCountId}`, formData.get(key));
            });
        }
    };
    // const endProcCate = formData.get('objectiveVar');
    let endProcObjective = null;
    if (objectiveVarID) {
        const [procObj] = Object.values(procConf).filter((proc) =>
            proc.getColumnById(objectiveVarID),
        );
        if (procObj) {
            endProcObjective = procObj.id;
        }
    }
    if (endProcObjective) {
        // transform end procs
        setEndProcs();
        formData.set('end_proc1', endProcObjective);
        valsSelected.forEach((value) => {
            formData.append('GET02_CATE_SELECT1', value);
        });

        formData.set('GET02_VALS_SELECT1', objectiveVarID);
        formData.delete('objectiveVar');
    }
    return formData;
};

// convert hsv color to rgb
const hsv2rgb = ({ h, s, v }, opacity = false) => {
    const f = (n, k = (n + h / 60) % 6) =>
        v - v * s * Math.max(Math.min(k, 4 - k, 1), 0);
    const rgb = [f(5), f(3), f(1)].map((value) => value * 255);
    if (opacity) {
        return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, 0.5)`;
    }
    return `rgb(${rgb[0]}, ${rgb[1]}, ${rgb[2]})`;
};

const downloadCsvFile = (fileName, csvContent) => {
    const header = 'data:text/csv;charset=utf-8-sig,';
    const encodedUri = encodeURI(header + csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    link.setAttribute('download', fileName);
    document.body.appendChild(link); // Required for FF

    link.click(); // This will download the data file named "my_data.csv".
    document.body.removeChild(link);
};

const getCurrentTimeStr = () => {
    const now = new Date();
    const offsetMs = now.getTimezoneOffset() * 60 * 1000;
    const dateLocal = new Date(now.getTime() - offsetMs);
    const timeStr = dateLocal
        .toISOString()
        .slice(0, 23)
        .replace(/[-:]/g, '')
        .replace('T', '_');
    return timeStr;
};

const downloadTextFile = (url, filename = null, option = {}) => {
    fetchWithLog(url, option)
        .then((response) => {
            return response.blob();
        })
        .then((blob) => {
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);

            if (filename) {
                if (blob.type === 'application/octet-stream') {
                    // zip
                    filename = filename.split('.');
                    filename[filename.length - 1] = 'zip';
                    filename = filename.join('.');
                }
                link.download = filename;
            }
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        })
        .catch(console.error);
};

// formData is FormData object
const chooseCyclicTraceTimeInterval = (formData) => {
    if (!formData) return formData;
    const traceTimes = [...formData.keys()].filter((e) =>
        e.match(/.+TraceTime\d+/),
    );
    const traceTimeOptions = traceTimes.map((e) => formData.get(e));

    const divisionNum = formData.get(CYCLIC_TERM.DIV_NUM);
    const intervalNum = formData.get(CYCLIC_TERM.INTERVAL);
    const windowsLengthNum = formData.get(CYCLIC_TERM.WINDOW_LENGTH);

    const targetDate =
        traceTimeOptions[0] === TRACE_TIME_CONST.RECENT
            ? moment().format('YYYY-MM-DD')
            : formData.get(CONST.STARTDATE);
    const targetTime =
        traceTimeOptions[0] === TRACE_TIME_CONST.RECENT
            ? moment().format('HH:mm')
            : formData.get(CONST.STARTTIME);

    // clear all datetimes before customize
    [CONST.STARTDATE, CONST.STARTTIME, CONST.ENDDATE, CONST.ENDTIME].forEach(
        (e) => formData.delete(e),
    );

    traceTimeOptions.forEach((traceTimeOption, i) => {
        const [startTimeRange, endTimeRange] =
            traceTimeOption === TRACE_TIME_CONST.FROM
                ? getEndTimeRange(
                      targetDate,
                      targetTime,
                      divisionNum,
                      intervalNum,
                      windowsLengthNum,
                  )
                : getStartTimeRange(
                      traceTimeOption,
                      targetDate,
                      targetTime,
                      divisionNum,
                      intervalNum,
                      windowsLengthNum,
                  );

        formData.append(CONST.STARTDATE, startTimeRange[0]);
        formData.append(CONST.STARTTIME, startTimeRange[1]);
        formData.append(CONST.ENDDATE, endTimeRange[0]);
        formData.append(CONST.ENDTIME, endTimeRange[1]);
    });

    return formData;
};

/**
 *
 * @param name
 * @param option: object {MIN: 0,
 *                        MAX: 0,
 *                        DEFAULT: 0}
 *
 */
function validateInputByNameWithOnchange(name, option) {
    if (!name) return;

    $(`input[name=${name}]`).off('change');
    $(`input[name=${name}]`).on('change', (e) => {
        // uncheck if disabled
        if (e.target.disabled) return;

        let { value } = e.currentTarget;
        if (!value || (value < option.MIN && value !== 0)) {
            e.currentTarget.value = option.MIN;
            showToastrMsg(i18nCommon.changedToMaxValue);
            e.target.focus();
        }
        if (value > option.MAX) {
            e.currentTarget.value = option.MAX;
            showToastrMsg(i18nCommon.changedToMaxValue);
            e.target.focus();
        }

        if (name === CYCLIC_TERM.INTERVAL || name === CYCLIC_TERM.DIV_OFFSET) {
            if (
                Math.abs(Number(value)) < option.DEFAULT &&
                value > option.MIN
            ) {
                e.currentTarget.value = option.DEFAULT;
                showToastrMsg(i18nCommon.changedToMaxValue);
                e.target.focus();
            }
        }
    });
}

const copyTextToClipboard = (url) => {
    fetchWithLog(url)
        .then((response) => response.blob())
        .then((blob) => {
            const reader = new FileReader();
            reader.onload = function () {
                writeClipboardText(reader.result);
            };
            reader.readAsText(blob);
        })
        .catch(console.error);
};

const getFilterItemDetail = (procCond, condition, filterType) => {
    const listCond = Array.isArray(condition)
        ? condition
        : condition !== 'NO_FILTER'
          ? [condition]
          : [];
    const filterItems = [];
    if (listCond.length) {
        listCond.forEach((item) => {
            const partNoFilters = getFilterByTypes(procCond, filterType);
            const filterItemIndex = partNoFilters[0].indexOf(Number(item));
            filterItems.push(partNoFilters[1][filterItemIndex]);
        });
    }
    return filterItems;
};

// get conditions
const getConditionFromSetting = (traceDat) => {
    const conditions = traceDat[CONST.COMMON][CONST.CONDT] || [];
    const filters = [];
    if (conditions.length) {
        conditions.forEach((condition) => {
            const filterInfo = {};
            const procCond = procConfigs[condition.cond_proc] || null;

            if (procCond) {
                filterInfo.proc_name = procCond ? procCond.name : '';
                filterInfo.items = {};

                if (condition[CONST.FILTER_PARTNO]) {
                    // partnumber
                    filterInfo.items.part_no = getFilterItemDetail(
                        procCond,
                        condition[CONST.FILTER_PARTNO],
                        filterTypes.PART_NO,
                    );
                }
                if (condition[CONST.FILTER_LINE]) {
                    // line
                    filterInfo.items.line = getFilterItemDetail(
                        procCond,
                        condition[CONST.FILTER_LINE],
                        filterTypes.LINE,
                    );
                }

                if (condition[CONST.FILTER_MACH]) {
                    // machine
                    filterInfo.items.machine = getFilterItemDetail(
                        procCond,
                        condition[CONST.FILTER_MACH],
                        filterTypes.MACHINE,
                    );
                }

                // TODO: filter others
                // filters.items.others = getFilterItemDetail(procCond, condition[CONST.FILTER_OTHER], CfgFilter.filterTypes.OTHER);
                filters.push(filterInfo);
            }
        });
    }
    return filters;
};

const genInfoTableBody = (traceDat) => {
    const firstEndProc = traceDat[CONST.ARRAY_FORMVAL].length
        ? traceDat[CONST.ARRAY_FORMVAL][0].end_proc
        : undefined;
    const startProc =
        procConfigs[traceDat[CONST.COMMON].start_proc] ||
        procConfigs[firstEndProc] ||
        undefined;
    const startProcName = startProc ? startProc.name : '';
    const startDate = _.isArray(traceDat[CONST.COMMON][CONST.STARTDATE])
        ? traceDat[CONST.COMMON][CONST.STARTDATE][0]
        : traceDat[CONST.COMMON][CONST.STARTDATE];
    const endDate = _.isArray(traceDat[CONST.COMMON][CONST.ENDDATE])
        ? traceDat[CONST.COMMON][CONST.ENDDATE][0]
        : traceDat[CONST.COMMON][CONST.ENDDATE];
    let startTime = _.isArray(traceDat[CONST.COMMON][CONST.STARTTIME])
        ? traceDat[CONST.COMMON][CONST.STARTTIME][0]
        : traceDat[CONST.COMMON][CONST.STARTTIME];
    let endTime = _.isArray(traceDat[CONST.COMMON][CONST.ENDTIME])
        ? traceDat[CONST.COMMON][CONST.ENDTIME][0]
        : traceDat[CONST.COMMON][CONST.ENDTIME];
    const startDateTime = `${startDate} ${startTime}`;
    const endDateTime = `${endDate} ${endTime}`;
    let settingDOM = '';
    const filters = getConditionFromSetting(traceDat);
    startTime = moment(`${startDateTime}Z`).format(DATE_FORMAT_WITHOUT_TZ);
    endTime = moment(`${endDateTime}Z`).format(DATE_FORMAT_WITHOUT_TZ);
    settingDOM += `<tbody>
                        <tr>
                            <td>${i18nCommon.startPoint}</td>
                            <td>${startProcName}</td>
                            <td></td>
                        </tr>
                        <tr>
                            <td>From</td>
                            <td>${startTime}</td>
                            <td></td>
                        </tr>
                        <tr>
                            <td>To</td>
                            <td>${endTime}</td>
                            <td></td>
                         </tr>`;

    if (filters.length) {
        filters.forEach((filter, k) => {
            // use filter label in first column of first filter item
            if (k === 0) {
                settingDOM += `<tr>
                    <td>${i18nCommon.filter}</td>
                    <td>${filter.proc_name}</td>
                    <td></td>
                </tr>`;
            } else {
                settingDOM += `<tr><td></td><td>${filter.proc_name}</td><td></td></tr>`;
            }
            if (filter.items.line && filter.items.line.length) {
                settingDOM += `<tr><td></td><td>${i18nCommon.line}</td><td>`;
                filter.items.line.forEach((line, k) => {
                    settingDOM += `${line || i18nCommon.allSelection}`;
                    if (k !== filter.items.line.length - 1) {
                        settingDOM += ', ';
                    }
                });
                settingDOM += '</td></tr>';
            }
            if (filter.items.machine && filter.items.machine.length) {
                settingDOM += `<tr><td></td><td>${i18nCommon.mach}</td><td>`;
                filter.items.machine.forEach((machine, k) => {
                    settingDOM += `${machine}`;
                    if (k !== filter.items.machine.length - 1) {
                        settingDOM += ', ';
                    }
                });
                settingDOM += '</td></tr>';
            }
            if (filter.items.part_no && filter.items.part_no.length) {
                settingDOM += `<tr><td></td><td>${i18nCommon.partNo}</td><td>`;
                filter.items.part_no.forEach((partNo, k) => {
                    settingDOM += `${partNo}`;
                    if (k !== filter.items.part_no.length - 1) {
                        settingDOM += ', ';
                    }
                });
                settingDOM += '</td></tr>';
            }
        });
    }
    settingDOM += '</tbody>';
    return settingDOM;
};

const copyDataPointInfo = (ele) => {
    $('#dp-info-content').removeClass('dp-opacity').addClass('dp-opacity');
    const clipboard = new ClipboardJS('#dp-info-table-copy');
    clipboard.on('success', (e) => {
        setTooltip(e.trigger, 'Copied!');
    });

    clipboard.on('error', (e) => {
        setTooltip(e.trigger, 'Failed!');
    });
    setTimeout(() => {
        $('#dp-info-content').removeClass('dp-opacity');
    }, 500);
};

const clipboardInit = () => {
    $('.clipboard').tooltip({
        trigger: 'click',
        placement: 'bottom',
    });

    const clipboard = new ClipboardJS('.clipboard');

    clipboard.on('success', (e) => {
        setTooltip(e.trigger, 'Copied!');
    });

    clipboard.on('error', (e) => {
        setTooltip(e.trigger, 'Failed!');
    });
};

const showInfoTable = (traceDat, tableId = 'setting-infor-table') => {
    const body = genInfoTableBody(traceDat);
    $(`#${tableId}`).html(body);

    // set copy clipboard for setting information
    clipboardInit();
};

const getLastNumberInString = (inputStr) => {
    const result = inputStr.match(/\d+$/);
    if (result) {
        return Number(result[0]);
    }

    return null;
};

const zipExport = (datasetId) => {
    const filename = $('#setting-name').text();
    const exportModeEle = $('[name=isExportMode]');
    const url = `/ap/api/fpp/zip_export?dataset_id=${datasetId}&bookmark_id=${exportModeEle.val()}`;
    downloadTextFile(url, `${filename}.zip`);
    exportModeEle.remove();
    return false;
};

const zipImport = () => {
    const filename = $('#importSelectFileInput').val();
    if (!filename) {
        return;
    }

    const url = `/ap/api/fpp/zip_import?filename=${filename}`;
    fetchWithLog(url)
        .then((response) => response.clone().json())
        .then((json) => {
            const userSettingId = json.id;
            const redirectPage = json.page;
            goToSettingPage(userSettingId, redirectPage, filename);
        })
        .catch(console.error);
};

const exportMode = () => {
    return !!$('[name=isExportMode]').val();
};

const handleZipExport = (res) => {
    // export mode
    if (!exportMode()) {
        return;
    }

    zipExport(res.isExportMode || res.responseJSON.dataset_id);
};

const rightClickHandler = (e, contextMenuEle) => {
    e.preventDefault();
    e.stopPropagation();

    // show context menu when right click heatmap
    const menu = $(contextMenuEle);
    const menuHeight = menu.height();
    const windowHeight = $(window).height();
    const left = e.clientX;
    let top = e.clientY;
    if (windowHeight - top < menuHeight) {
        top -= menuHeight;
    }
    menu.css({
        left: `${left}px`,
        top: `${top}px`,
        display: 'block',
    });

    return false;
};

const overrideUiSortable = () => {
    // override jQuery UI draggable
    const mouseCopy = $.extend({}, $.ui.mouse.prototype);

    $.extend($.ui.mouse.prototype, {
        _mouseInit: function () {
            const that = this;
            if (!this.options.mouseButton) {
                this.options.mouseButton = 1;
            }

            mouseCopy._mouseInit.apply(this, arguments);

            if (this.options.mouseButton === 3) {
                this.element.bind(`contextmenu.${this.widgetName}`, (event) => {
                    if (
                        $.data(
                            event.target,
                            `${that.widgetName}.preventClickEvent`,
                        ) === true
                    ) {
                        $.removeData(
                            event.target,
                            `${that.widgetName}.preventClickEvent`,
                        );
                        event.stopImmediatePropagation();
                        return false;
                    }
                    event.preventDefault();
                    return false;
                });
            }

            this.started = false;
        },
        _mouseDown: function (event) {
            if (this.options.mouseButton === 1) {
                const isGraphTarget = event.target.closest('.nsewdrag.drag');
                if (isGraphTarget) return;
            }

            // we may have missed mouseup (out of window)
            this._mouseStarted && this._mouseUp(event);

            this._mouseDownEvent = event;

            const that = this;
            const btnIsLeft = event.which === this.options.mouseButton;
            // event.target.nodeName works around a bug in IE 8 with
            // disabled inputs (#7620)
            const elIsCancel =
                typeof this.options.cancel === 'string' && event.target.nodeName
                    ? $(event.target).closest(this.options.cancel).length
                    : false;
            if (!btnIsLeft || elIsCancel || !this._mouseCapture(event)) {
                return true;
            }

            this.mouseDelayMet = !this.options.delay;
            if (!this.mouseDelayMet) {
                this._mouseDelayTimer = setTimeout(() => {
                    that.mouseDelayMet = true;
                }, this.options.delay);
            }

            if (this._mouseDistanceMet(event) && this._mouseDelayMet(event)) {
                this._mouseStarted = this._mouseStart(event) !== false;
                if (!this._mouseStarted) {
                    event.preventDefault();
                    return true;
                }
            }

            // Click event may never have fired (Gecko & Opera)
            if (
                $.data(event.target, `${this.widgetName}.preventClickEvent`) ===
                true
            ) {
                $.removeData(
                    event.target,
                    `${this.widgetName}.preventClickEvent`,
                );
            }

            // these delegates are required to keep context
            this._mouseMoveDelegate = function (event) {
                return that._mouseMove(event);
            };
            this._mouseUpDelegate = function (event) {
                return that._mouseUp(event);
            };
            $(document)
                .bind(`mousemove.${this.widgetName}`, this._mouseMoveDelegate)
                .bind(`mouseup.${this.widgetName}`, this._mouseUpDelegate);

            event.preventDefault();

            mouseHandled = true;
            return true;
        },
    });
};

const getDateTimeRangeFromCyclic = (formData, traceTimeOption) => {
    const divisionNum = formData.get(CYCLIC_TERM.DIV_NUM);
    const intervalNum = formData.get(CYCLIC_TERM.INTERVAL);
    const windowsLengthNum = formData.get(CYCLIC_TERM.WINDOW_LENGTH);
    const datetimeVal = $('#cyclicTermDatetimePicker').val();

    const targetDate =
        traceTimeOption === TRACE_TIME_CONST.RECENT
            ? moment().format('YYYY-MM-DD')
            : moment(datetimeVal).format('YYYY-MM-DD');
    const targetTime =
        traceTimeOption === TRACE_TIME_CONST.RECENT
            ? moment().format('HH:mm')
            : moment(datetimeVal).format('HH:mm');

    const [startTimeRange, endTimeRange] =
        traceTimeOption === TRACE_TIME_CONST.FROM
            ? getEndTimeRange(
                  targetDate,
                  targetTime,
                  divisionNum,
                  intervalNum,
                  windowsLengthNum,
              )
            : getStartTimeRange(
                  traceTimeOption,
                  targetDate,
                  targetTime,
                  divisionNum,
                  intervalNum,
                  windowsLengthNum,
              );
    return `${startTimeRange[0]} ${startTimeRange[1]}${DATETIME_PICKER_SEPARATOR}${endTimeRange[0]} ${endTimeRange[1]}`;
};
const genDatetimeRange = (formData, traceTimeName = 'traceTime') => {
    const divideOption = $('#divideOption').val();
    let datetimeRange;
    let divideDiv = null;
    let traceTimeOption = '';
    if (divideOption) {
        divideDiv = $(`#for-${divideOption}`);
        traceTimeOption = divideDiv.find(`[name*=TraceTime]:checked`).val();
    } else {
        traceTimeOption = formData.get(traceTimeName);
    }
    const useLatestTime = traceTimeOption === TRACE_TIME_CONST.RECENT;
    const isCyclicTerm = divideOption === CYCLIC_TERM.NAME;
    const isCyclicCalender =
        divideOption && divideOption === divideOptions.cyclicCalender;
    if (isCyclicCalender) {
        datetimeRange = [$('#datetimeRangeShowValue').text()];
    } else {
        datetimeRange = formData.getAll('DATETIME_RANGE_PICKER');
    }
    const timeKeys = [
        CONST.STARTDATE,
        CONST.STARTTIME,
        CONST.ENDDATE,
        CONST.ENDTIME,
    ];
    // delete all old value
    timeKeys.forEach((key) => {
        formData.delete(key);
    });
    const seperator = DATETIME_PICKER_SEPARATOR;

    if (isCyclicTerm) {
        // get datetime range from ~ to
        // for stp, scp, rlp cyclic term
        const pickedTimeRange = $('#datetimeRangeShowValue').text();
        if (pickedTimeRange !== '') {
            datetimeRange = [pickedTimeRange];
        } else {
            datetimeRange = [
                getDateTimeRangeFromCyclic(formData, traceTimeOption),
            ];
        }
    } else if (useLatestTime && !isCyclicCalender) {
        // in case of PCA, only apply for Target data
        const lastDatetimeEles = datetimeRange.length - 1;
        datetimeRange[lastDatetimeEles] = '';
        const timeUnit = formData.get('timeUnit') || 60;
        const recentTimeInterval = formData.get('recentTimeInterval') || 24;
        if (!isEmpty(recentTimeInterval)) {
            datetimeRange[lastDatetimeEles] = calcLatestDateTime(
                timeUnit,
                recentTimeInterval,
            );
        }
    }
    datetimeRange.forEach((timeRange) => {
        if (timeRange !== '') {
            const [starting, ending] = timeRange.split(seperator);
            if (starting && ending) {
                const [startDate, startTime] = starting && starting.split(' ');
                const [endDate, endTime] = ending.split(' ');
                const startUTCDt = toUTCDateTime(
                    startDate,
                    startTime || '00:00',
                );
                const endUTCDt = toUTCDateTime(endDate, endTime || '00:00');
                formData.append(timeKeys[0], startUTCDt.date);
                formData.append(timeKeys[1], startUTCDt.time);
                formData.append(timeKeys[2], endUTCDt.date);
                formData.append(timeKeys[3], endUTCDt.time);
            }
        }
    });
    // formData.delete('DATETIME_RANGE_PICKER');
    // formData.delete('DATETIME_PICKER');

    formData = setFitlerIntoFormData(formData);
    formData = bindCategoryParams(formData);

    return formData;
};

const calcLatestDateTime = (timeUnit, recentTimeInterval) => {
    let timeDiffMinute, newStartDate, newEndDate, newStartTime, newEndTime;

    if (['months', 'years'].includes(timeUnit)) {
        newStartDate = moment()
            .subtract(recentTimeInterval, timeUnit)
            .format(DATE_FORMAT);
        newStartTime = moment()
            .subtract(recentTimeInterval, timeUnit)
            .format(TIME_FORMAT);
    } else {
        timeDiffMinute = Number(recentTimeInterval) * Number(timeUnit);
        newStartDate = moment()
            .add(-timeDiffMinute, 'minute')
            .format(DATE_FORMAT);
        newStartTime = moment()
            .add(-timeDiffMinute, 'minute')
            .format(TIME_FORMAT);
    }
    newEndDate = moment().format(DATE_FORMAT);
    newEndTime = moment().format(TIME_FORMAT);
    return `${newStartDate} ${newStartTime}${DATETIME_PICKER_SEPARATOR}${newEndDate} ${newEndTime}`;
};

const generateDefaultNameExport = () => {
    const title = document.title ? document.title.split(' ')[0] : '';
    try {
        const endProc1 = $('select[name*=end_proc]')[0].value;
        const endProc = endProc1 ? procConfigs[endProc1].name : '';
        let dateTimeRange = '';

        const hasDivision = $('select[name=compareType]').length > 0;

        if (hasDivision) {
            dateTimeRange = $('#datetimeRangeShowValue').text();
        } else {
            dateTimeRange = getDateTimeRangeValue('var', 'traceTime', false);
        }

        const { startDate, startTime, endDate, endTime } =
            splitDateTimeRange(dateTimeRange);

        return `${title}_${endProc}_${startDate}-${startTime.split(':').join('')}_${endDate}-${endTime.split(':').join('')}`;
    } catch (e) {
        return `${title}_${moment().format('YYYYMMDD_HH:mm')}`;
    }
};

const handleLoadSettingBtns = () => {
    // show only load button and hide bookmark button
    $('.load-setting-common').hide();
    $('.load-setting-tile-page').show();
};

const calMinMaxYScale = (minY, maxY, scaleOption) => {
    const margin = (maxY - minY) * 0.1;
    if (
        scaleOption === scaleOptionConst.THRESHOLD ||
        scaleOption === 'scale_threshold'
    ) {
        return [minY - margin, maxY + margin];
    }

    return [minY, maxY];
};

const fetchData = async (url, data, method = 'GET', options = {}) => {
    const headers = {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        content_type: 'json',
        ...options.headers,
    };

    const requestContent = {
        url,
        data,
        type: method,
        headers: headers,
        cache: false,
        ...options,
    };

    return new Promise((resolve, reject) => {
        ajaxWithLog({
            ...requestContent,
            success: (res) => {
                res = jsonParse(res);
                resolve(res);
            },
            error: (err) => {
                reject(err);
            },
        });
    });
};

const makeUID = () => {
    return Date.now().toString(36) + Math.random().toString(36).substring(2);
};

const handleBeforeSendRequestToShowGraph = (jqXHR, formdata) => {
    const threadID = makeUID();
    xhr = jqXHR;
    xhr.thread_id = threadID;
    formdata.set('thread_id', threadID);
    const jumpId = getParamFromUrl('jump_key');
    const objectiveVar = getParamFromUrl('objective_var');
    const excludedColumns = getParamFromUrl('excluded_columns');
    if (jumpId) {
        formdata.set('jump_key', jumpId);
    }
    if (objectiveVar) {
        formdata.set('objective_var', objectiveVar);
    }

    if (excludedColumns) {
        formdata.set('excluded_columns', excludedColumns);
    }
    return formdata;
};

const afterReceiveResponseCommon = (res) => {
    dataSetID = res['dataset_id'];
};

const sortableTable = (
    tableID,
    filterCols = [],
    maxheight = null,
    scrollToBottom = false,
    iconSort = true,
) => {
    const tableIDEl = `#${tableID}`;
    const table = $(tableIDEl);

    if (table.length <= 0) return;

    // ad sort icon in header
    const heades = $(table.find('thead tr')[0]).find('th');
    const hasFilterRow = table.find('thead .filter-row').length > 0;

    const ths = [];
    heades.each((i, th) => {
        const thEl = $(th);
        const thClass = thEl.attr('class') || '';
        if (iconSort) {
            thEl.addClass('position-relative');
            const iconHtml = `<span id="sortCol-${i}" idx="${i}" class="mr-1 sortCol" title="Sort"><i id="asc-${i}" class="fa fa-sm fa-play asc"></i><i id="desc-${i}" class="fa fa-sm fa-play desc"></i></span>`;
            thEl.append(iconHtml);
        }

        if (filterCols && filterCols.length > 0 && !hasFilterRow) {
            const hasFilter = filterCols.indexOf(i) !== -1;
            const filterTh = `<th scope="col" class="search-box ${thClass}">${hasFilter ? `<input class="form-control filterCol" data-col-idx="${i}" placeholder="${i18nCommon.search}...">` : ''}</th>`;
            ths.push(filterTh);
        }
    });

    if (iconSort) {
        initSortIcon();
    }

    // add filter
    if (filterCols) {
        const trFilter = `<tr id="filters" class="filter-row">
                                ${ths.join('')}
                           </tr>`;
        $(table.find('thead')).append(trFilter);
        $(table.find('thead .filter-row:not(:first)')).remove();

        handleSearchFilterInTable(tableID);
    }

    if (maxheight) {
        tableScroll(tableID, maxheight, scrollToBottom);
    }
};

const initSortIcon = () => {
    // handle sort
    $('.sortCol').off('click');
    $('.sortCol').on('click', (el) => {
        let asc = true;
        const sortEl = $(el.target.closest('.sortCol'));
        const isFirstClick = sortEl.attr('clicked');
        if (isFirstClick) {
            asc = false;
            sortEl.removeAttr('clicked');
        } else {
            sortEl.attr('clicked', '0');
        }

        const idx = sortEl.attr('idx');

        if (asc) {
            sortEl.removeClass('desc');
            sortEl.addClass('asc');
        } else {
            sortEl.removeClass('asc');
            sortEl.addClass('desc');
        }

        // Reset sort status in other cols
        const thisTable = sortEl.closest('table');
        const otherSortCols = thisTable.find(`.sortCol:not([idx=${idx}])`);
        otherSortCols.removeAttr('clicked');
        otherSortCols.removeClass('asc');
        otherSortCols.removeClass('desc');

        const table = sortEl.parents('table').eq(0);
        const rows = table.find('tbody tr').toArray().sort(comparer(idx, asc));

        for (let i = 0; i < rows.length; i++) {
            table.append(rows[i]);
        }
    });
};

const handleSearchFilterInTable = (tableID) => {
    convertTextH2Z('#' + tableID);
    $(`#${tableID} .filterCol`).on('keyup change clear search', function () {
        const colIdx = $(this).data('col-idx');
        const value = stringNormalization($(this).val().toLowerCase());
        let regex = makeRegexForSearchCondition(value);
        regex = new RegExp(regex, 'i');
        $(`#${tableID} tbody tr`).filter(function f() {
            const colVal = getCellValue(this, colIdx);
            $(this).toggle(regex.test(colVal));
        });

        scrollToBottom(`${tableID}_wrap`);
    });
};

const getCellValue = (row, index) => {
    const cell = $(row).children('td').eq(index);
    let colVal = '';
    const selects = $(cell).find('select');
    if (selects.length > 0) {
        colVal += selects.find('option:selected').text();
    }
    const input = $(cell).find('input');
    if (input.length > 0) {
        colVal += input.val();
    }

    const textArea = $(cell).find('textarea');
    if (textArea.length > 0) {
        colVal += textArea.text();
    }

    if (textArea.length === 0 && input.length === 0 && selects.length === 0) {
        colVal += $(cell).text();
    }

    return colVal;
};

const comparer = (index, asc) => {
    return (a, b) => {
        const valA = getCellValue(a, index);
        const valB = getCellValue(b, index);
        if (!asc) {
            return $.isNumeric(valA) && $.isNumeric(valB)
                ? valB - valA
                : valB.toString().localeCompare(valA);
        }
        return $.isNumeric(valA) && $.isNumeric(valB)
            ? valA - valB
            : valA.toString().localeCompare(valB);
    };
};

const tableScroll = (tblID, maxHeight, toBottom = false) => {
    const tableIDEl = `#${tblID}`;
    const table = $(tableIDEl);

    const h =
        maxHeight && maxHeight.toString().includes('%')
            ? maxHeight
            : `${maxHeight}px`;

    table.addClass('table-fixed');
    table.wrap(
        `<div id="${tblID}_wrap" class="table-responsive" style="max-height: ${h}"></div>`,
    );

    if (toBottom) {
        scrollToBottom(`${tblID}_wrap`);
    }
};

/**
 * @description The function that will create threshold line
 * @return lines array to assign to shapes in plotly chart.
 * @param xThreshold: (Object) Threshold param of x axis (vertical line line)
 * @param yThreshold: (Object) Threshold param of Y axis (horizontal line)
 * @param ref: (Object) xaxis -> xref, yaxis -> yref.
 * @param xRange: array has two elements of x0 and x1.
 * @param yRange: array has two elements of y0 and y1.
 */
const genThresholds = (
    xThreshold = {},
    yThreshold = {},
    ref = {
        xaxis: 'paper',
        yaxis: 'y',
    },
    xRange = [0, 1],
    yRange = [0, 1],
) => {
    const lines = [];

    const xLow = xThreshold && xThreshold['thresh-low'];
    const xHigh = xThreshold && xThreshold['thresh-high'];
    const xPrcMin = xThreshold && xThreshold['prc-min'];
    const xPrcMax = xThreshold && xThreshold['prc-max'];

    const yLow = yThreshold && yThreshold['thresh-low'];
    const yHigh = yThreshold && yThreshold['thresh-high'];
    const yPrcMin = yThreshold && yThreshold['prc-min'];
    const yPrcMax = yThreshold && yThreshold['prc-max'];
    const hasValue = (value) => {
        return value !== null && value !== undefined;
    };
    if (hasValue(xLow)) {
        lines.push({
            type: 'line',
            xref: ref.xaxis,
            yref: ref.yaxis,
            x0: xLow,
            y0: yRange[0],
            x1: xLow,
            y1: yRange[1],
            line: {
                color: CONST.RED,
                width: 0.75,
            },
        });
    }
    if (hasValue(xHigh)) {
        lines.push({
            type: 'line',
            xref: ref.xaxis,
            yref: ref.yaxis,
            x0: xHigh,
            y0: yRange[0],
            x1: xHigh,
            y1: yRange[1],
            line: {
                color: CONST.RED,
                width: 0.75,
            },
        });
    }
    if (hasValue(yLow)) {
        lines.push({
            type: 'line',
            xref: ref.xaxis,
            yref: ref.yaxis,
            x0: xRange[0],
            y0: yLow,
            x1: xRange[1],
            y1: yLow,
            line: {
                color: CONST.RED,
                width: 0.75,
            },
        });
    }
    if (hasValue(yHigh)) {
        lines.push({
            type: 'line',
            xref: ref.xaxis,
            yref: ref.yaxis,
            x0: xRange[0],
            y0: yHigh,
            x1: xRange[1],
            y1: yHigh,
            line: {
                color: CONST.RED,
                width: 0.75,
            },
        });
    }
    if (hasValue(xPrcMin)) {
        lines.push({
            type: 'line',
            xref: ref.xaxis,
            yref: ref.yaxis,
            x0: xPrcMin,
            y0: yRange[0],
            x1: xPrcMin,
            y1: yRange[1],
            line: {
                color: CONST.BLUE,
                width: 0.75,
            },
        });
    }
    if (hasValue(xPrcMax)) {
        lines.push({
            type: 'line',
            xref: ref.xaxis,
            yref: ref.yaxis,
            x0: xPrcMax,
            y0: yRange[0],
            x1: xPrcMax,
            y1: yRange[1],
            line: {
                color: CONST.BLUE,
                width: 0.75,
            },
        });
    }
    if (hasValue(yPrcMin)) {
        lines.push({
            type: 'line',
            xref: ref.xaxis,
            yref: ref.yaxis,
            x0: xRange[0],
            y0: yPrcMin,
            x1: xRange[1],
            y1: yPrcMin,
            line: {
                color: CONST.BLUE,
                width: 0.75,
            },
        });
    }
    if (hasValue(yPrcMax)) {
        lines.push({
            type: 'line',
            xref: ref.xaxis,
            yref: ref.yaxis,
            x0: xRange[0],
            y0: yPrcMax,
            x1: xRange[1],
            y1: yPrcMax,
            line: {
                color: CONST.BLUE,
                width: 0.75,
            },
        });
    }

    return lines;
};

const changeNoDataLinkSelection = (selection = true) => {
    // check startProc has noLinkData option (StP, RLP, CHM)
    const hasNoDataLink = Array.from(
        $('select#start_proc option').map((k, v) => $(v).val()),
    ).includes('0');
    if (!hasNoDataLink) {
        return;
    }
    const noDataLinkValue = selection ? '0' : '';
    const isNoDataLink = $('select#start_proc').val() === '0';
    const isBlankOption = $('select#start_proc').val() === '';
    // no facet, or no filter - sp = blank: to nolink
    if (selection && isBlankOption) {
        $('select#start_proc').val(noDataLinkValue).change();
    }
    // with facet, or filter cond - sp = nolink: to blank
    if (!selection && isNoDataLink) {
        $('select#start_proc').val(noDataLinkValue).change();
    }
};

const bindFacetChangeEvents = () => {
    if (!$('select#start_proc')) {
        return;
    }

    // requirement #3
    const isUseFacet = checkHasFacet();
    const isUseFilter = checkHasFilter();

    // if remove facet and has no filter,change from blank to データ紐付なし
    // if (!isUseFacet && !isUseFilter) {
    //     changeNoDataLinkSelection(true);
    // }

    // if facet selected, change from データ紐付なし -> blank
    if (isUseFacet) {
        changeNoDataLinkSelection(false);
    }
    const isShowScatter = checkShowScatter();
    if (isUseFacet && isShowScatter) {
        changeShowScatter(!isShowScatter);
    }
};

const changeShowScatter = (isShowing) => {
    $('input[name=showScatterPlotSelect]').prop('checked', isShowing);
};
const checkShowScatter = () => {
    return $('input[name=showScatterPlotSelect]').is(':checked');
};

const checkHasFacet = () => {
    let hasFacet = false;
    $('select[name=catExpBox]').each((k, elm) => {
        if ($(elm).val()) {
            hasFacet = true;
        }
    });

    return hasFacet;
};

const bindFilterChangeEvents = (selectedProc) => {
    const endProcs = $('select[name^="end_proc"]');
    if (!endProcs.length) {
        return;
    }
    const getEndProcID = (itemID) => {
        if (!endProcs[itemID].value) {
            return getEndProcID(itemID + 1);
        }
        return endProcs[itemID].value;
    };
    // requirement #4
    // if filter condition != start process & startProc == データ紐付なし
    // update: choose any filter cond, change to link data
    // clear noLinkData option
    if (selectedProc) {
        changeNoDataLinkSelection(false);
    }
};

const checkHasFilter = () => {
    const afterClearDOM = $('select[name^="cond_proc"]');
    const filterValues = Array.from(
        afterClearDOM.map((i, filter) => $(filter).val()),
    ).filter((i) => i !== '').length;

    return filterValues;
};

const bindRemoveFilterCondByCards = () => {
    // get all valid filter conds
    const hasFilter = checkHasFilter();
    const hasFacet = checkHasFacet();
    // if remove all filter cond, change to no link data, else keep previous status
    if (!hasFilter && !hasFacet) {
        changeNoDataLinkSelection();
    }
};

const bindFilterRemoveEvents = (filterDOMID) => {
    // requirement #4
    // if clear/ remove filter condition
    // change from blank value to データ紐付なし
    const selectDOM = $(filterDOMID).find('select[name^="cond_proc"]');
    selectDOM.each((k, select) => {
        $(select)
            .off('select2:clear')
            .on('select2:clear', function (e) {
                bindRemoveFilterCondByCards();
            });
    });
};

const validateDataLink = (ele) => {
    const procVal = $('select#start_proc').val();
    const prevVal = $(ele).attr('data-prev-proc-id') || '';
    const isNoDataLink = procVal === '0';
    const isUseFacet = checkHasFacet();
    const isUseFilter = checkHasFilter();
    if (isNoDataLink && (isUseFacet || isUseFilter)) {
        // change to prev value of start proc
        // todo: show message to user
        // need to confirm with PO before apply
        // $('select#start_proc').val(prevVal).change();
    } else {
        $(ele).attr('data-prev-proc-id', procVal);
    }
};

const onChangeDivideOption = () => {
    let selectedCatExpBox = [];
    $('#divideOption').on('change', (e) => {
        // check if there Div was selected.
        selectedCatExpBox = $('select[name=catExpBox] option:selected').filter(
            (i, el) => el.value,
        );
        const isSelectedDiv = [...selectedCatExpBox]
            .map((el) => el.value)
            .includes(facetLevels.DIV);
        const { value } = e.currentTarget;
        if (value !== 'category' && isSelectedDiv) {
            const categoryDivideText = $(
                'select[name=compareType] option[value=category]',
            ).text();
            const confirmText =
                i18nCommon.changeDivideOptionConfirmText.replaceAll(
                    'CATEGORY_DIVIDE_OPTION',
                    categoryDivideText,
                );
            $('#changeDivideOptionConfirmMessage').html(confirmText);
            $('#changeCompareTypeConfirm').modal().toggle();
        }
    });

    // when click cancel, let app select category divide.
    $('#changeCompareTypeConfirmCancel').click(() => {
        $('#divideOption').val('category').trigger('change');
    });

    // When click ok, the app clear selected Div and change divide option
    $('#changeCompareTypeConfirmOK').click(() => {
        selectedCatExpBox.each((i, el) => {
            if (el.value === facetLevels.DIV) {
                el.parentNode.value = '';
            }
        });
    });
};

const onChangeDivInFacet = () => {
    let currentSelectedDiv = null;
    $('select[name=catExpBox]').on('change');
    $('select[name=catExpBox]').on('change', (e) => {
        const { value } = e.currentTarget;
        currentSelectedDiv = $(e.currentTarget);
        const currentDivideOption = $('select[name=compareType]').val();
        const currentDivideOptionText = $(
            'select[name=compareType] option:selected',
        ).text();
        const categoryDivideText = $(
            'select[name=compareType] option[value=category]',
        ).text();

        if (value === facetLevels.DIV && currentDivideOption !== 'category') {
            const confirmText = i18nCommon.changeDivConfirmText
                .replaceAll('CURRENT_DIVIDE_OPTION', currentDivideOptionText)
                .replaceAll('CATEGORY_DIVIDE_OPTION', categoryDivideText);
            $('#changeDivConfirmationText').html(confirmText);
            $('#changeDivInFacetConfirm').modal().toggle();
        }

        // trigger change division number
        $(`input[name=${CYCLIC_TERM.DIV_NUM}]`).trigger('change');
    });

    $('#changeDivInFacetConfirmOK').on('click', () => {
        // automatically change divide option to Category
        $('#divideOption').val('category').trigger('change');
    });

    $('#changeDivInFacetConfirmCancel').on('click', () => {
        // clear div variable.
        currentSelectedDiv.val('');
    });
};

const findKeyFromFormdata = (key, formData, withAll = false) => {
    const keys = [...formData.keys()];
    let result = keys.filter((k) => k.includes(key));
    if (withAll) {
        result = result.filter((k) => k !== 'All');
    }
    return formData.getAll(result[0]);
};

const checkTrue = (value) => {
    return value !== null && value !== undefined;
};

const toLocalTime = (datetime) => {
    return moment.utc(datetime).local().format(DATETIME_FORMAT);
};

const getFirstSelectedProc = () => {
    const startProcId = $('select[name^=start_proc]').first().val();
    const endProcId = $('select[name^=end_proc]').first().val();

    return !startProcId || startProcId === '0' ? endProcId : startProcId;
};

const getExportDataSrc = () => {
    const exportDataInput = $(EXPORT_DOM.INPUT).val();
    return exportDataInput || EXPORT_DATA_SRC.ALL;
};

const exportData = async (url, type, formData) => {
    // type = csv | tsv
    loadingShow();
    const queryString = genQueryStringFromFormData(formData);
    const endpoint = `${url}?${queryString}`;
    const filename = `${generateDefaultNameExport()}.${type}`;
    await downloadTextFile(endpoint, filename);
    loadingHide();
    showToastrStartedCSV();
};

const tsvClipBoard = async (url, formData) => {
    const queryString = genQueryStringFromFormData(formData);
    const endPoint = `${url}?${queryString}`;
    copyTextToClipboard(endPoint);
    return false;
};

const handleExportDataCommon = (
    type,
    formData,
    url = '/ap/api/common/data_export',
) => {
    const exportFrom = getExportDataSrc();
    formData.set('export_from', exportFrom);
    if (type === EXPORT_TYPE.CSV) {
        exportData(`${url}/csv`, 'csv', formData);
    }

    if (type === EXPORT_TYPE.TSV) {
        exportData(`${url}/tsv`, 'tsv', formData);
    }

    if (type === EXPORT_TYPE.TSV_CLIPBOARD) {
        tsvClipBoard(`${url}/tsv`, formData);
    }
};

const makeRegexForSearchCondition = (searchStr) => {
    if (/^\[\s*\d+\s*-\s*\d+\s*\]$/.test(searchStr)) {
        const newSearchStr = [];
        let [from, to] = searchStr.split('-');
        from = from.replace('[', '');
        to = to.replace(']', '');

        const zeroPadding = to.length - 1;
        let zeroStr = '1';
        for (let i = 0; i < zeroPadding; i++) {
            zeroStr += '0';
        }
        const maxVal = Number(zeroStr);
        from = Number(from);
        to = Number(to);
        for (let i = from; i <= to; i++) {
            let val = (i / maxVal).toString().replace('.', '');
            if (val.length < zeroPadding + 1) {
                const offset = zeroPadding + 1 - val.length;
                for (let y = 0; y < offset; y++) {
                    val += '0';
                }
            }

            newSearchStr.push(val);
        }
        return newSearchStr.join('|');
    }

    if (/((\|+\s*){2,})|[^\||\|$]/.test(searchStr)) {
        let strs = searchStr.split('|');
        strs = strs.filter((val) => val.length > 0);
        return strs.join('|');
    }

    return searchStr;
};

const showGraphAndDumpData = (exportType, callback) => {
    const dataSrc = getExportDataSrc();
    const isFullPage = dataSrc === 'all';
    if (isFullPage) {
        callback(exportType, dataSrc);
    } else {
        if (isGraphShown) {
            callback(exportType, dataSrc);
        } else {
            // click show graph button
            $(currentFormID).find('button.show-graph').trigger('click');
            checkShownGraphInterval = setInterval(() => {
                if (isGraphShown) {
                    callback(exportType, dataSrc);
                    clearInterval(checkShownGraphInterval);
                }
            }, 500);
        }
    }
};
const isDefined = (variable) => {
    return typeof variable !== 'undefined';
};

function create_UUID() {
    var dt = new Date().getTime();
    var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(
        /[xy]/g,
        function (c) {
            var r = (dt + Math.random() * 16) % 16 | 0;
            dt = Math.floor(dt / 16);
            return (c == 'x' ? r : (r & 0x3) | 0x8).toString(16);
        },
    );
    return uuid;
}

const getEndProcCfg = async () => {
    if (
        !processId ||
        !procConfigs[processId] ||
        !procConfigs[processId].is_use_dummy_datetime
    ) {
        return false;
    }
    const endProcCfg = procConfigs[processId];
    await endProcCfg.getCTRange();
    return endProcCfg;
};

const updateDatetimeInputs = (value) => {
    const datetimeRanges = $('input[name=DATETIME_RANGE_PICKER]:visible');
    const singleDatetime = $('input[name=DATETIME_PICKER]:visible');

    if (singleDatetime.length) {
        singleDatetime.val(value).change();
    }
    if (datetimeRanges.length) {
        datetimeRanges.each((i, datetimeInput) => {
            const currentVal = $(datetimeInput).val();
            if (currentVal) {
                $(datetimeInput).val(value).change();
            }
        });
    }
};

const updateXOption = (serialOrder = true) => {
    if (isSettingLoading) return;
    let xOptionVal = 'INDEX';
    if (!serialOrder && !isSettingLoading) {
        xOptionVal = 'TIME';
    }
    if ($('#xOption').length) {
        $('#xOption').data('change-val-only', true);
        $('#xOption').val(xOptionVal).trigger('change');
    }
};

const changeDefaultIndexOrdering = async () => {
    if (isSettingLoading) return;
    const endProcCfg = await getEndProcCfg();
    if (endProcCfg && endProcCfg.ct_range.length) {
        // in FPP, change xOption in case of CT is dummy datetime
        updateXOption();
        // change default value of datetime picker
        const [minDate, maxDate] = endProcCfg.ct_range;
        if (minDate && maxDate) {
            const startDate = convertUTCToLocaltime(minDate); // floor
            const endDate = convertUTCToLocaltime(maxDate); // ceil
            const dummyDatetimeRange = `${roundMinute(startDate, 'down')} ${DATETIME_PICKER_SEPARATOR} ${roundMinute(endDate, 'up')}`;
            // $('input[name=DATETIME_RANGE_PICKER]').val(dummyDatetimeRange).change();
            updateDatetimeInputs(dummyDatetimeRange);
        }
    }
    // reset to show xOption modal
    $('#xOption').data('change-val-only', false);
};

if (!String.prototype.format) {
    String.prototype.format = function () {
        const args = arguments;
        return this.replace(/{(\d+)}/g, function (match, number) {
            return typeof args[number] != 'undefined' ? args[number] : match;
        });
    };
}

const convertStringToNumber = (stringNumber = '') => {
    const _stringNumber = String(stringNumber).replaceAll(',', '');
    const number = Number(_stringNumber);
    return number == _stringNumber ? number : stringNumber;
};

const roundMinute = (dateStr, option = 'up', unit = 5) => {
    if (unit === 0) return moment(dateStr).format('YYYY-MM-DD HH:00');
    let minute = moment(dateStr).minute();
    const isNeedRound = minute % unit !== 0;
    if (!isNeedRound || !minute || !unit) return dateStr;

    let divideUnit = (minute - (minute % unit)) / unit;
    if (option === 'up') {
        // ceil minute
        divideUnit = divideUnit + 1;
    }

    const newMinute = divideUnit * unit;
    return moment(moment(dateStr).format('YYYY-MM-DD HH:00'))
        .add(newMinute, 'minutes')
        .format(DATE_TIME_FMT);
};
const reselectVariablesToShowGraph = () => {
    $('input[name^=selectedVar]').each((_v, el) => {
        const elStatus = $(el).is(':checked');
        const guiDOM = $(
            `input[name^=GET02_VALS_SELECT][value=${$(el).val()}]`,
        );
        const guiStatus = guiDOM.prop('checked');

        if (elStatus !== guiStatus) {
            guiDOM.prop('checked', elStatus).trigger('change');
        }
    });
    if (reselectCallback) {
        const jumpKey = getParamFromUrl('jump_key');
        if (jumpKey) {
            const excludedColumns = [
                ...$(domEles.problematicTbl).find(
                    'input[name^=selectedVar]:not(:checked)',
                ),
            ].map((el) => Number($(el).val()));
            const url = new URL(window.location.href);
            url.searchParams.set('excluded_columns', excludedColumns.join(','));
            window.history.replaceState(null, null, url);
        }
        reselectCallback(true);
    }
};

const genProblematicContent = (plotData, multipleTimeRange = false) => {
    const problematicTblBody = $(domEles.problematicTbl).find('tbody');
    const problematicPCATblBody = $(domEles.problematicPCATbl).find('tbody');
    problematicTblBody.html('');
    problematicPCATblBody.html('');
    if (multipleTimeRange) {
        $(domEles.problematicPCATbl).show();
        $(domEles.problematicTbl).hide();

        const selectedVars = plotData.train_data.selected_vars;
        const nullPercent = plotData.train_data.null_percent;
        const selectedVarsTarget = plotData.target_data.selected_vars;
        const nullPercentTarget = plotData.target_data.null_percent;

        const trainDataSensorIDs = Object.keys(selectedVars).length
            ? Object.keys(selectedVars)
            : null;
        const trainDataSensorIDsFromNADict = Object.keys(nullPercent).length
            ? Object.keys(nullPercent)
            : null;
        const targetDataSensorIDs = Object.keys(selectedVarsTarget).length
            ? Object.keys(selectedVarsTarget)
            : null;
        const targetDataSensorIDsFromNADict = Object.keys(nullPercentTarget)
            .length
            ? Object.keys(nullPercentTarget)
            : null;
        const selectedCols =
            trainDataSensorIDs ||
            targetDataSensorIDs ||
            trainDataSensorIDsFromNADict ||
            targetDataSensorIDsFromNADict;

        if (selectedCols) {
            selectedCols.forEach((colID) => {
                const colName =
                    selectedVars[colID] || selectedVarsTarget[colID] || '';
                const zeroVar = plotData.train_data.zero_variance
                    .map((i) => String(i))
                    .includes(colID);
                const naRate = trainDataSensorIDsFromNADict
                    ? applySignificantDigit(nullPercent[colID])
                    : 100;
                const selectedTrain = naRate <= 50 && !zeroVar;

                const zeroVarTarget = plotData.target_data.zero_variance
                    .map((i) => String(i))
                    .includes(colID);
                const naRateTarget = targetDataSensorIDsFromNADict
                    ? applySignificantDigit(nullPercentTarget[colID])
                    : 100;
                const selectedTarget = naRateTarget <= 50 && !zeroVarTarget;

                const selected =
                    selectedTrain && selectedTarget ? ' checked' : '';
                const rowContent = `<tr>
                    <td>
                        <div class="custom-control custom-radio">
                          <input type="radio" id="selectedVar_${colID}" name="selectedVar_${colID}" 
                            class="custom-control-input" ${selected} value="${colID}">
                            <label class="custom-control-label" onclick="uncheckRadioEvent(this);" for="selectedVar_${colID}"></label>
                        </div>
                    </td>
                    <td style="text-align: left; padding-left: 4px">${colName}</td>
                    <td>${zeroVar ? '〇' : ''}</td>
                    <td>${naRate}%</td>
                    <td>${zeroVarTarget ? '〇' : ''}</td>
                    <td>${naRateTarget}%</td>
                </tr>`;
                problematicPCATblBody.append(rowContent);
            });
        }
    } else {
        $(domEles.problematicPCATbl).hide();
        $(domEles.problematicTbl).show();

        const selectedVars = Object.keys(plotData.selected_vars).length
            ? Object.keys(plotData.selected_vars)
            : null;
        const nullPercent = Object.keys(plotData.null_percent).length
            ? Object.keys(plotData.null_percent)
            : null;

        const selectedCols = selectedVars || nullPercent;
        selectedCols.forEach((colID) => {
            const colName = plotData.selected_vars[colID] || '';
            const zeroVar = plotData.zero_variance
                .map((i) => String(i))
                .includes(colID);
            const naRate = nullPercent
                ? applySignificantDigit(plotData.null_percent[colID])
                : 0;
            const selected = naRate <= 50 && !zeroVar ? ' checked' : '';
            const rowContent = `<tr>
                <td>
                    <div class="custom-control custom-radio">
                      <input type="radio" id="selectedVar_${colID}" name="selectedVar_${colID}" 
                        class="custom-control-input" ${selected} value="${colID}">
                        <label class="custom-control-label" onclick="uncheckRadioEvent(this);" for="selectedVar_${colID}"></label>
                    </div>
                </td>
                <td style="text-align: left; padding-left: 4px">${colName}</td>
                <td>${zeroVar ? '〇' : ''}</td>
                <td>${naRate}%</td>
            </tr>`;
            problematicTblBody.append(rowContent);
        });
    }
};

const showRemoveProblematicColsMdl = (
    resPlotData = null,
    multipleTimeRange = false,
) => {
    setTimeout(() => {
        genProblematicContent(resPlotData, multipleTimeRange);
        $(domEles.problematicModal).modal().show();
    }, 1000);
};

const genSelect2Param = (type = 1, data = []) => {
    const params = {
        width: '100%',
        allowClear: true,
        tags: true,
        placeholder: '',
    };
    if (type === 2) {
        params.multiple = true;
        params.tokenSeparators = [' ', '\n'];
    }
    if (data && data.length) {
        params.data = data;
    }
    return params;
};

const uncheckRadioEvent = (e) => {
    const currentEl = $(e).parent().find('input');
    const check = currentEl.prop('checked');
    if (check) {
        setTimeout(() => {
            currentEl.prop('checked', false).trigger('change');
        }, 100);
    }
};

const formatCatExpBox = (facetLabel, facetFormat) => {
    // format cat exp box
    if (facetFormat.length >= 1) {
        facetLabel = facetLabel.toString().split('|');
        const facetFormat1 = facetFormat[0];
        const facetFormat2 = facetFormat[1];
        const facet1 = facetLabel[0];
        const facet2 = facetLabel[1];
        if (facetFormat1 && facet1 && facet1 !== COMMON_CONSTANT.NA) {
            facetLabel[0] = applySignificantDigit(
                Number(facet1),
                undefined,
                facetFormat1,
            );
        }
        if (facetFormat2 && facet2 && facet2 !== COMMON_CONSTANT.NA) {
            facetLabel[1] = applySignificantDigit(
                Number(facet2),
                undefined,
                facetFormat2,
            );
        }
        facetLabel = facetLabel.join(' | ');
    }
    return facetLabel;
};

const detectColumnsContainReservedNull = (masterInfo = {}, masterName = '') => {
    const addExceptionCases = (list = [], masterName = '') => {
        if (
            ![
                masterNameColumnDict.__LINE_GROUP__,
                masterNameColumnDict.__EQUIP_GROUP__,
                masterNameColumnDict.__PART_TYPE__,
            ].includes(masterName)
        )
            return;

        const idKey = masterName.split('_')[0];
        for (const suffix of [
            targetColumnNameSuffixes.name_jp,
            targetColumnNameSuffixes.name_en,
            targetColumnNameSuffixes.name_local,
            targetColumnNameSuffixes.abbr_jp,
            targetColumnNameSuffixes.abbr_en,
            targetColumnNameSuffixes.abbr_local,
        ]) {
            list.push(`${idKey}_${suffix}`);
        }
    };

    // Start - Detect columns that contain reserved null value
    const suffixes = Object.values(targetColumnNameSuffixes);
    let reservedNull = false;
    const targetReservedNullColumnNames = [];
    if ('id' in masterInfo && [1, '1'].includes(masterInfo['id'])) {
        reservedNull = true;
        targetReservedNullColumnNames.push(
            ...suffixes.map((e) => `${masterNameColumnDict[masterName]}_${e}`),
        );
        addExceptionCases(
            targetReservedNullColumnNames,
            masterNameColumnDict[masterName],
        );
    }

    let parentReservedNull = false;
    const parentIdKeys = Object.keys(masterInfo).filter((n) =>
        n.includes('_id'),
    );
    const targetParentReservedNullColumnNames = [];
    if (parentIdKeys.length > 0) {
        for (let parentIdKey of parentIdKeys) {
            if ([1, '1'].includes(masterInfo[parentIdKey])) {
                parentReservedNull = true;
                parentIdKey = parentIdKey.replace('_id', '');
                targetParentReservedNullColumnNames.push(
                    ...suffixes.map((e) => `${parentIdKey}_${e}`),
                );
                addExceptionCases(
                    targetParentReservedNullColumnNames,
                    parentIdKey,
                );
            }
        }
    }
    // End - Detect columns that contain reserved null value

    return [
        reservedNull,
        targetReservedNullColumnNames,
        parentReservedNull,
        targetParentReservedNullColumnNames,
    ];
};

const getMasterInfoHovering = (masterId, masterName) => {
    if (!masterId) {
        return {};
    }
    const currentTraceData = graphStore.getTraceData();
    const masterInfo = currentTraceData.master_info[masterName][masterId];
    let i18nMasterInfo = {};

    const [
        reservedNull,
        targetReservedNullColumnNames,
        parentReservedNull,
        targetParentReservedNullColumnNames,
    ] = detectColumnsContainReservedNull(masterInfo, masterName);

    for (const key in masterInfo) {
        const i18nKey = $('#' + key).text() || key;
        let value = masterInfo[key];
        if (value == null) {
            value = ''; // replace null to blank
        }

        if (key === 'id' || key.includes('_id')) {
            // No show id field when hover
            consoleLogDebug(`[Hover] ${key}: ${value}`);
            continue;
        }

        const is_reserved_null =
            (reservedNull && targetReservedNullColumnNames.includes(key)) ||
            (parentReservedNull &&
                targetParentReservedNullColumnNames.includes(key));

        i18nMasterInfo[i18nKey] = [value, is_reserved_null];
    }

    return i18nMasterInfo;
};

const updatePriority = (tableID) => {
    $(`${tableID} tbody tr`).each((rowIdx, row) => {
        let tdChildElement = 'td:nth-child(1)';
        if (tableID === jumpEls.jumpTblID) {
            tdChildElement = 'td:nth-child(3)';
        }
        $(row)
            .find(tdChildElement)
            .text(rowIdx + 1);
    });
};

// convert dimension name to short name with ".."
const shortTextName = (text, limitSize = 21) => {
    // total length from name
    let nameLength = 0;
    let shortName = '';
    for (const char of [...text]) {
        nameLength += new Blob([char]).size;
        if (nameLength > limitSize) {
            break;
        }
        shortName += char;
    }
    if (nameLength >= limitSize) {
        shortName += '...';
    }
    return shortName;
};

const getFmtValueOfArrayTrim5Percent = (array) => {
    let sortedArray = [...array].sort();
    const start = Math.floor(sortedArray.length * 0.05);
    const end = sortedArray.length - start;
    sortedArray = sortedArray.slice(start, end);
    const fmt =
        sortedArray.length > 0
            ? significantDigitFmt(Math.max(...sortedArray))
            : '';
    return fmt;
};

const getFmtValueOfArray = (array) => {
    const decimal = '.';
    let sortedArray = [...array].sort();
    let usageNum = 0;
    let sigDigit = 0;
    sortedArray.forEach((num) => {
        const vals = String(num).split(decimal);
        if (vals.length > 1 && vals[1].length > sigDigit) {
            sigDigit = vals[1].length;
            usageNum = num;
        }
    });
    const fmt =
        sortedArray.length > 0 ? significantDigitFmt(usageNum, sigDigit) : '';
    return fmt === ',.1f' ? ',.2f' : fmt;
};

const alignLengthTickLabels = (ticks) => {
    const decimal = '.';
    const pattern = /0*$/;
    try {
        const zeroLens = ticks
            .filter((tick) => tick.label && tick.label !== '')
            .map((tick) => {
                // tick > 0 then pass
                if (!tick.label || !tick.label.includes(decimal)) return -1;
                const [, subTicks] = tick.label.split(decimal);
                const matching = subTicks.match(pattern);
                if (!matching) return 0;
                return matching[0].length;
            });
        const minLen = Math.min(...zeroLens);
        if (minLen > 0) {
            ticks.map((tick) => {
                tick.label = tick.label.substring(
                    0,
                    tick.label.length - minLen,
                );
                const lastIdx = tick.label.length - 1;
                const decimalIdx = tick.label.lastIndexOf(decimal);
                // remove decimal if no number after that (10.)
                if (decimalIdx >= 0 && decimalIdx == lastIdx) {
                    tick.label = tick.label.substring(0, lastIdx);
                }
            });
        }
    } catch (e) {
        return;
    }
};
const clearEmptyEndProcs = (formData) => {
    const emptyEndProcKeys = [];
    for (const pair of formData.entries()) {
        if (pair[0].startsWith('end_proc') && !pair[1]) {
            emptyEndProcKeys.push(pair[0]);
        }
    }
    emptyEndProcKeys.forEach((ngKey) => formData.delete(ngkey));

    // add start proc key
    if (!formData.get('start_proc')) {
        formData.set('start_proc', []);
    }
    return formData;
};

const jspreadsheetCustomHooks = () => {
    const onbeforechange = (instance, cell, x, y, value) =>
        stringNormalization(value);
    const onpaste = (instance) => {
        const history = instance.jspreadsheet.history.at(-1);
        const { oldStyle, records } = history;

        const readOnlyStates = {};

        // turn off readonly so we can modify style
        for (const { x, y } of records) {
            const cellName = jspreadsheet.getColumnNameFromId([x, y]);
            readOnlyStates[cellName] =
                instance.jspreadsheet.isReadOnly(cellName);
            instance.jspreadsheet.setReadOnly(cellName, false);
        }

        // rollback style
        instance.jspreadsheet.setStyle(oldStyle);

        // rollback read only state
        for (const { x, y } of records) {
            const cellName = jspreadsheet.getColumnNameFromId([x, y]);
            instance.jspreadsheet.setReadOnly(
                cellName,
                readOnlyStates[cellName],
            );
        }
    };

    return { onbeforechange, onpaste };
};

const bindNominalSelection = (formData, clearOnFlyFilter) => {
    const nominalScaleInput = $('input[name=is_nominal_scale]');
    formData.delete('nominal_vars');
    // check if GUI has nominal input as first item
    if (nominalScaleInput[0]) {
        const isNominalScale = nominalScaleInput.is(':checked') ? '1' : '0';
        formData.set('is_nominal_scale', isNominalScale);

        // bind list of nominal variables from modal
        const nominalVars = $('input[name=graph_nominal_scale]');
        if (nominalVars.length && !clearOnFlyFilter) {
            formData.delete('nominal_vars');
            const nominalValues = [];
            let selectedNominalVars = [];
            selectedNominalVars = Array.from(
                nominalVars.filter((i, item) => $(item).is(':checked')),
            );
            selectedNominalVars.forEach((item, i) => {
                nominalValues.push($(item).val());
            });
            formData.set('nominal_vars', JSON.stringify(nominalValues));
        }
    }
    return formData;
};

const showGraphWithNominalSetting = () => {
    // hide nominal_scale setting modal
    $('#nominalScaleModal').modal('hide');
    callToBackEndAPI(false);
};
const convertDatetimePreview = (dateStr) => {
    // YYYY-MM-DD => YYYY-MM-DD 00:00:00 (Done)
    //
    // MM-DD => YYYY-MM-DD 00:00:00 (YYYY=this year. Curretnly 2001.) ok
    //
    // YYYY/MM/DD => YYYY-MM-DD 00:00:00 ok
    //
    // MM/DD => YYYY-MM-DD 00:00:00 (YYYY=this year. Curretnly 2001.)
    //
    // YYYY年MM月DD日hh時mm分ss秒 => YYYY-MM-DD hh:mm:ss
    //
    // YYYY年MM月DD日 => YYYY-MM-DD 00:00:00
    //
    // YY年MM月DD日 => YYYY-MM-DD 00:00:00 (YYYY=this century.)
    //
    // MM月DD日 => YYYY-MM-DD 00:00:00 (YYYY=this year.) ←これはLubridateでは変換できなかった。 ok

    // case 1: MM-DD/ MM月DD日/ MM/DD
    // case 2: YYYY/MM/DD/ YYYY-MM-DD/ YYYY年MM月DD日
    // case 3:  YYYY年MM月DD日hh時mm分ss秒
    // case 4: YY年MM月DD日

    const regex1 = /^(\d{1,2}-\d{1,2}|\d{1,2}\/\d{1,2}|\d{1,2}月\d{1,2}日)$/;
    const regex2 =
        /^(\d{4}-\d{1,2}-\d{1,2}|\d{4}\/\d{1,2}\/\d{1,2}|\d{4}年\d{1,2}月\d{1,2}日)$/;
    const regex3 = /^\d{4}年\d{1,2}月\d{1,2}日\d{1,2}時\d{1,2}分\d{1,2}秒$/;
    const regex4 =
        /^(\d{1,2}-\d{1,2}-\d{1,2}|\d{1,2}\/\d{1,2}\/\d{1,2}|\d{1,2}年\d{1,2}月\d{1,2}日)$/;

    let dateFormat = dateStr;

    const getMatched = (dateStr) => {
        const matched = dateStr.split(/-|\/|月|日|年|時|分|秒/);
        return matched.filter((v) => v).map((v) => addZeroToNumber(Number(v)));
    };

    if (regex1.test(dateStr)) {
        const matched = getMatched(dateStr);
        dateFormat = moment().format(
            `YYYY-${matched[0]}-${matched[1]} 00:00:00`,
        );
    }

    if (regex2.test(dateStr)) {
        const matched = getMatched(dateStr);
        dateFormat = `${matched[0]}-${matched[1]}-${matched[2]} 00:00:00`;
    }

    if (regex3.test(dateStr)) {
        const matched = getMatched(dateStr);
        dateFormat = `${matched[0]}-${matched[1]}-${matched[2]} ${matched[3]}:${matched[4]}:${matched[5]}`;
    }

    if (regex4.test(dateStr)) {
        const matched = getMatched(dateStr);
        const YY = moment().format('YYYY').slice(0, 2);
        dateFormat = `${YY}${matched[0]}-${matched[1]}-${matched[2]} 00:00:00`;
    }

    return dateFormat;
};

const visualTextLength = (text, textSize = 14) => {
    const span = `<div class="hide-element" style="font-size: ${textSize}px; font-family: 'Arial'">${text}</div>`;
    $('html').append(span);
    const width = $('.hide-element').width();
    $('.hide-element').remove();

    return Math.floor(width);
};

const trimTextLengthByPixel = (text, length = 150, textSize = 14) => {
    let tmp = text;
    let trimmed = text;
    if (visualTextLength(tmp, textSize) > length) {
        trimmed += '...';
        while (visualTextLength(trimmed, textSize) > length) {
            tmp = tmp.substring(0, tmp.length - 1);
            trimmed = tmp + '...';
        }
    }

    return trimmed;
};

const convertNumberByThousandSep = (numberValue) => {
    const hasThousandSep = !!numberValue.match(THOUSAND_SEP_PATTERN);
    if (hasThousandSep) {
        return numberValue.replaceAll(',', '');
    }
    return '';
};

const getNValueInArray = (array, n) => {
    if (n > array.length) return array;
    const nextIndex = array.length / n < 2 ? 2 : Math.floor(array.length / n);
    const res = [];
    let i = 0;
    while (i < array.length) {
        res.push(array[i]);
        i += nextIndex;
    }

    return res;
};

/**
 * Change Background Color
 * @param {HTMLElement} ele
 */
const changeBackgroundColor = (ele) => {
    if (
        [
            DataTypes.STRING.bs_value,
            DataTypes.REAL_SEP.bs_value,
            DataTypes.EU_REAL_SEP.bs_value,
        ].includes(ele.getAttribute('value'))
    ) {
        $(ele)
            .closest('.config-data-type-dropdown')
            .find('[name=dataType]')
            .css('color', 'orange');
    } else {
        $(ele)
            .closest('.config-data-type-dropdown')
            .find('[name=dataType]')
            .css('color', 'white');
    }
};

/**
 * Check the path is folder or file
 * @param path
 * @return {Promise<{
 *   status: number,
 *   isExist: boolean,
 *   isFile: boolean,
 *   isFolder: boolean,
 * }>}
 */
function checkFolderOrFile(path) {
    const data = { path: path };
    return fetchData(
        '/ap/api/setting/check_folder_or_file',
        JSON.stringify(data),
        'POST',
    );
}

/**
 * Download Text as a file
 * @param {string?} fileName - a file name with extension
 * @param {string} content - a string contains content of download file
 * @return {void} - A file will be downloaded on browser
 */
function downloadText(fileName, content) {
    // Create a temporary <a> element to trigger the download
    const a = document.createElement('a');
    try {
        a.href =
            'data:text/tab-separated-values;charset=utf-8,' +
            encodeURIComponent(content);
        a.download = fileName ?? 'data';
        document.body.appendChild(a);
        a.click();
    } catch (e) {
        console.error(e);
    } finally {
        // Clean up
        document.body.removeChild(a);
    }
}
