/**
 * @file Contains all constant that serve for data type dropdown.
 * @author Pham Minh Hoang <hoangpm6@fpt.com>
 * @author Tran Thi Kim Tuyen <tuyenttk5@fpt.com>
 */

/**
 * Class contains all constant that serves for data type dropdown menu control
 */
class DataTypeDropdown_Constant {
    /**
     * A dictionary that contains title for each data type
     */
    static RawDataTypeTitle = Object.freeze({
        REAL: document.getElementById(DataTypes.REAL.i18nLabelID).textContent,
        TEXT: document.getElementById(DataTypes.TEXT.i18nLabelID).textContent,
        DATETIME: document.getElementById(DataTypes.DATETIME.i18nLabelID).textContent,
        SMALL_INT: document.getElementById(DataTypes.SMALL_INT.i18nLabelID)
            .textContent,
        INTEGER: document.getElementById('i18nInteger_Int32').textContent,
        BIG_INT: document.getElementById(DataTypes.BIG_INT.i18nLabelID).textContent,
        CATEGORY: document.getElementById(DataTypes.CATEGORY.i18nLabelID).textContent,
        BOOLEAN: document.getElementById(DataTypes.BOOLEAN.i18nLabelID).textContent,
        DATE: document.getElementById(DataTypes.DATE.i18nLabelID).textContent,
        TIME: document.getElementById(DataTypes.TIME.i18nLabelID).textContent,
    });

    /**
     * Get from api ap/api/setting/show_latest_records
     * @type {Readonly<DataGroupType>}
     */
    static DataGroupType = Object.freeze({
        DATETIME: 1,
        MAIN_SERIAL: 2,
        SERIAL: 3,
        DATETIME_KEY: 4,
        DATE: 5,
        TIME: 6,
        MAIN_DATE: 7,
        MAIN_TIME: 8,
        INT_CATE: 10,
        LINE_NAME: 20,
        LINE_NO: 21,
        EQ_NAME: 22,
        EQ_NO: 23,
        PART_NAME: 24,
        PART_NO: 25,
        ST_NO: 26,
        GENERATED: 99,
        GENERATED_EQUATION: 100,
    });

    /**
     * A list of element names
     */
    static ElementNames = Object.freeze({
        englishName: 'englishName',
        systemName: 'systemName',
        japaneseName: 'japaneseName',
        localName: 'localName',
        dataType: 'dataType',
        unit: 'unit',
    });

    /**
     * A list of attributes that be limited to be selected one time
     */
    static UnableToReselectAttrs = Object.freeze([
        'is_get_date',
        'is_auto_increment',
        'is_main_date',
        'is_main_time',
    ]);

    /**
     * A list of attributes that be limited to select only one 1 column / process
     */
    static AllowSelectOneAttrs = Object.freeze([
        'is_get_date',
        'is_main_date',
        'is_main_time',
        'is_main_serial_no',
        'is_line_name',
        'is_line_no',
        'is_eq_name',
        'is_eq_no',
        'is_part_name',
        'is_part_no',
        'is_st_no',
        'is_auto_increment',
    ]);

    /**
     * A list of attributes for column type
     */
    static ColumnTypeAttrs = Object.freeze([
        'is_get_date',
        'is_main_date',
        'is_main_time',
        'is_serial_no',
        'is_main_serial_no',
        'is_auto_increment',
        'is_line_name',
        'is_line_no',
        'is_eq_name',
        'is_eq_no',
        'is_part_name',
        'is_part_no',
        'is_st_no',
        'is_int_cat',
    ]);

    /**
     * A list of data types that allow applying format
     */
    static AllowFormatingDataType = Object.freeze([
        DataTypes.SMALL_INT.bs_value,
        DataTypes.SMALL_INT_SEP.bs_value,
        DataTypes.EU_SMALL_INT_SEP.bs_value,

        DataTypes.INTEGER.bs_value,
        DataTypes.INTEGER_SEP.bs_value,
        DataTypes.EU_INTEGER_SEP.bs_value,

        DataTypes.BIG_INT.bs_value,
        DataTypes.BIGINT_SEP.bs_value,
        DataTypes.EU_BIGINT_SEP.bs_value,

        DataTypes.REAL.bs_value,
        DataTypes.REAL_SEP.bs_value,
        DataTypes.EU_REAL_SEP.bs_value,
    ]);

     /**
     * A datatype default object
     * @type Readonly<DataTypeObject>
     */
    static DataTypeDefaultObject = Object.freeze({
        value: '',
        is_get_date: false,
        is_main_date: false,
        is_main_time: false,
        is_serial_no: false,
        is_main_serial_no: false,
        is_auto_increment: false,
        is_int_cat: false,
    });
}
