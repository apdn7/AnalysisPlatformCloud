/**
 * @file Contains core functions that serve for data type dropdown.
 * @author Pham Minh Hoang <hoangpm6@fpt.com>
 * @author Tran Thi Kim Tuyen <tuyenttk5@fpt.com>
 */

/**
 * Class contains core function of dropdown menu
 */
class DataTypeDropdown_Core extends DataTypeDropdown_Event {
    /**
     * generate data type dropdown list Html
     * @param {number} idx
     * @param {DataTypeObject} defaultValue
     * @param {string} getKey
     * @param {?boolean} isRegisteredMainDatetimeColumn
     * @return {string} - string HTML of dropdown
     */
    static generateHtml(
        idx = 0,
        defaultValue = this.DataTypeDefaultObject,
        getKey,
        isRegisteredMainDatetimeColumn = false,
    ) {
        let text;
        if (['is_main_date', 'is_main_time', 'is_get_date'].includes(getKey)) {
            text = datatypeI18nText[getKey];
            if (_.isObject(text)) {
                text = text[defaultValue.value];
            }
        } else {
            text = this.RawDataTypeTitle[defaultValue.raw_data_type];
        }
        const setClassForSelectedItem = (itemValue) =>
            defaultValue.raw_data_type === itemValue ? 'active' : '';
        const attrKey = getKey
            ? `${getKey}="true" column_type=${masterDataGroup[mappingDataGroupType[getKey]]} data-attr-key=${getKey}`
            : '';

        return `
<div
    class="multi-level-dropdown config-data-type-dropdown config-data-type-dropdown_${idx}"
>
    <button
        class="btn btn-default dropdown-toggle"
        type="button"
        ${defaultValue.is_master_col || isRegisteredMainDatetimeColumn ? 'disabled' : ''}
    >
        <span
            class="csv-datatype-selection row-item for-search"
            name="${procModalElements.dataType}"
            id="dataTypeShowValue_${idx}"
            value="${defaultValue.value ?? ''}"
            is-registered-col="${defaultValue.isRegisteredCol ?? ''}"
            is-big-int="${defaultValue.is_big_int ?? ''}"
            is_get_date="${defaultValue.is_get_date ?? ''}"
            data-origin-raw-data-type="${defaultValue.raw_data_type ?? ''}"
            data-raw-data-type="${defaultValue.raw_data_type ?? ''}"
            data-attr-key="${getKey}"
            ${attrKey}
            ${defaultValue.checked}
        >${text}</span>
    </button>
    <div class="data-type-selection">
        <div class="data-type-selection-content data-type-selection-left">
            <div class="data-type-selection-box">
                <span class="data-type-selection-title">${$(procModali18n.i18nSpecial).text()}</span>
                <ul>
                    <li
                        class="dataTypeSelection"
                        is_get_date
                        value="${DataTypes.DATETIME.bs_value}"
                        data-type="${DataTypes.DATETIME.name}"
                        raw-data-type="${DataTypes.DATETIME.bs_value}"
                    >${$(procModali18n.i18nMainDatetime).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_main_date
                        value="${DataTypes.DATE.bs_value}"
                        data-type="${DataTypes.DATE.name}"
                        raw-data-type="${DataTypes.DATE.bs_value}"
                    >${$(procModali18n.i18nMainDate).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_main_time
                        value="${DataTypes.TIME.bs_value}"
                        data-type="${DataTypes.TIME.name}"
                        raw-data-type="${DataTypes.TIME.bs_value}"
                    >${$(procModali18n.i18nMainTime).text()}</li>
                    <!-- BridgeStation no need to show below column types
                    <li
                        class="dataTypeSelection"
                        is_main_serial_no
                        value="${DataTypes.INTEGER.name}"
                        data-type="${DataTypes.INTEGER.name}"
                    >${$(procModali18n.i18nMainSerialInt).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_main_serial_no
                        value="${DataTypes.TEXT.name}"
                        data-type="${DataTypes.TEXT.name}"
                    >${$(procModali18n.i18nMainSerialStr).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_auto_increment
                        value="${DataTypes.DATETIME.name}"
                        data-type="${DataTypes.DATETIME.name}"
                    >${$(procModali18n.i18nDatetimeKey).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_serial_no
                        value="${DataTypes.INTEGER.name}"
                        data-type="${DataTypes.INTEGER.name}"
                    >${$(procModali18n.i18nSerialInt).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_serial_no
                        value="${DataTypes.TEXT.name}"
                        data-type="${DataTypes.TEXT.name}"
                    >${$(procModali18n.i18nSerialStr).text()}</li>
                    -->
                </ul>
            </div>
            <div class="data-type-selection-box d-none">
                <span class="data-type-selection-title">${$(procModali18n.i18nFilterSystem).text()}</span>
                <ul>
                    <li
                        class="dataTypeSelection"
                        is_line_name
                        value="${DataTypes.TEXT.name}"
                        data-type="${DataTypes.TEXT.name}"
                    >${$(procModali18n.i18nLineNameStr).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_line_no
                        value="${DataTypes.INTEGER.name}"
                        data-type="${DataTypes.INTEGER.name}"
                    >${$(procModali18n.i18nLineNoInt).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_eq_name
                        value="${DataTypes.TEXT.name}"
                        data-type="${DataTypes.TEXT.name}"
                    >${$(procModali18n.i18nEqNameStr).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_eq_no
                        value="${DataTypes.INTEGER.name}"
                        data-type="${DataTypes.INTEGER.name}"
                    >${$(procModali18n.i18nEqNoInt).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_part_name
                        value="${DataTypes.TEXT.name}"
                        data-type="${DataTypes.TEXT.name}"
                    >${$(procModali18n.i18nPartNameStr).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_part_no
                        value="${DataTypes.INTEGER.name}"
                        data-type="${DataTypes.INTEGER.name}"
                    >${$(procModali18n.i18nPartNoInt).text()}</li>
                    <li
                        class="dataTypeSelection"
                        is_st_no
                        value="${DataTypes.INTEGER.name}"
                        data-type="${DataTypes.INTEGER.name}"
                    >${$(procModali18n.i18nStNoInt).text()}</li>
                </ul>
            </div>
        </div>
        <div class="data-type-selection-content data-type-selection-right">
            <div class="data-type-selection-box">
                <span class="data-type-selection-title">${$(procModali18n.i18nDatatype).text()}</span>
                <ul>
                    <li
                        class="dataTypeSelection"
                        only-datatype
                        value="${DataTypes.REAL.bs_value}"
                        data-type="${DataTypes.REAL.name}"
                        raw-data-type="${DataTypes.REAL.bs_value}"
                    >${$('#' + DataTypes.REAL.i18nLabelID).text()}</li>
                    <li
                        class="dataTypeSelection"
                        only-datatype
                        value="${DataTypes.SMALL_INT.bs_value}"
                        data-type="${DataTypes.SMALL_INT.name}"
                        raw-data-type="${DataTypes.SMALL_INT.bs_value}"
                    >${$('#' + DataTypes.SMALL_INT.i18nLabelID).text()}</li>
                    <li class="dropdown-submenu">
                        <!-- BS support multi integer type (int16, int32, int64) -->
                        <span 
                            data-name="${DataTypes.SMALL_INT.bs_value}"
                        >${$('#' + 'i18nInteger_Others').text()}</span>
                        <ul class="dropdown-menu">
                            <li
                                class="dataTypeSelection"
                                only-datatype
                                value="${DataTypes.SMALL_INT.bs_value}"
                                data-type="${DataTypes.SMALL_INT.name}"
                                raw-data-type="${DataTypes.SMALL_INT.bs_value}"
                            >${$('#' + DataTypes.SMALL_INT.i18nLabelID).text()}</li>
                            <li
                                class="dataTypeSelection"
                                only-datatype
                                value="${DataTypes.INTEGER.bs_value}"
                                data-type="${DataTypes.INTEGER.name}"
                                raw-data-type="${DataTypes.INTEGER.bs_value}"
                            >${$('#' + 'i18nInteger_Int32').text()}</li>
                            <li
                                class="dataTypeSelection"
                                only-datatype
                                value="${DataTypes.BIG_INT.bs_value}"
                                data-type="${DataTypes.BIG_INT.name}"
                                raw-data-type="${DataTypes.BIG_INT.bs_value}"
                            >${$('#' + DataTypes.BIG_INT.i18nLabelID).text()}</li>
                            <!-- BS support int64 -->
                        </ul>
                    </li>
                    <li
                        class="dataTypeSelection"
                        only-datatype
                        value="${DataTypes.TEXT.bs_value}"
                        data-type="${DataTypes.TEXT.name}"
                        raw-data-type="${DataTypes.TEXT.bs_value}"
                    >${$('#' + DataTypes.TEXT.i18nLabelID).text()}</li>
                    <li
                        class="dataTypeSelection"
                        only-datatype
                        value="${DataTypes.CATEGORY.bs_value}"
                        data-type="${DataTypes.CATEGORY.name}"
                        raw-data-type="${DataTypes.CATEGORY.bs_value}"
                    >${$('#' + DataTypes.CATEGORY.i18nLabelID).text()}</li>
                    <li
                        class="dataTypeSelection"
                        only-datatype
                        value="${DataTypes.DATETIME.bs_value}"
                        data-type="${DataTypes.DATETIME.name}"
                        raw-data-type="${DataTypes.DATETIME.bs_value}"
                    >${$('#' + DataTypes.DATETIME.i18nLabelID).text()}</li>
                    <li
                        class="dataTypeSelection"
                        only-datatype
                        value="${DataTypes.DATE.bs_value}"
                        data-type="${DataTypes.DATE.name}"
                        raw-data-type="${DataTypes.DATE.bs_value}"
                    >${$('#' + DataTypes.DATE.i18nLabelID).text()}</li>
                    <li
                        class="dataTypeSelection"
                        only-datatype
                        value="${DataTypes.TIME.bs_value}"
                        data-type="${DataTypes.TIME.name}"
                        raw-data-type="${DataTypes.TIME.bs_value}"
                    >${$('#' + DataTypes.TIME.i18nLabelID).text()}</li>
                    <li class="dropdown-submenu">
                        <!-- BS support multi LOGICAL type (boolean, judge) -->
                        <span
                            data-value="${DataTypes.LOGICAL.bs_value}"
                            data-name="${DataTypes.LOGICAL.name}"
                        >${$('#' + DataTypes.LOGICAL.i18nLabelID).text()}</span>
                        <ul class="dropdown-menu">
                            <li
                                class="dataTypeSelection ${setClassForSelectedItem(DataTypes.BOOLEAN.bs_value)}"
                                only-datatype
                                value="${DataTypes.BOOLEAN.bs_value}"
                                data-type="${DataTypes.BOOLEAN.name}"
                                raw-data-type="${DataTypes.BOOLEAN.bs_value}"
                            >${$('#' + DataTypes.BOOLEAN.i18nLabelID).text()}</li>
                            <li
                                class="dataTypeSelection ${setClassForSelectedItem(DataTypes.JUDGE.bs_value)} d-none"
                                only-datatype
                                value="${DataTypes.JUDGE.bs_value}"
                                data-type="${DataTypes.JUDGE.name}"
                                raw-data-type="${DataTypes.JUDGE.bs_value}"
                            >${$('#' + DataTypes.JUDGE.i18nLabelID).text()}</li>
                            <!-- BS not support yet -->
                        </ul>
                    </li>
                    <li
                        class="dataTypeSelection d-none"
                        only-datatype
                        value="${DataTypes.REAL_SEP.name}"
                        data-type="${DataTypes.REAL.name}"
                        raw-data-type="${DataTypes.REAL_SEP.bs_value}"
                    >${$('#' + DataTypes.REAL_SEP.i18nLabelID).text()}</li>
                    <!-- BS not support yet -->
                    <li
                        class="dataTypeSelection d-none"
                        only-datatype
                        value="${DataTypes.INTEGER_SEP.name}"
                        data-type="${DataTypes.INTEGER.name}"
                        raw-data-type="${DataTypes.INTEGER_SEP.bs_value}"
                    >${$('#' + DataTypes.INTEGER_SEP.i18nLabelID).text()}</li>
                    <!-- BS not support yet -->
                    <li
                        class="dataTypeSelection d-none"
                        only-datatype
                        value="${DataTypes.EU_REAL_SEP.name}"
                        data-type="${DataTypes.REAL.name}"
                        raw-data-type="${DataTypes.EU_REAL_SEP.bs_value}"
                    >${$('#' + DataTypes.EU_REAL_SEP.i18nLabelID).text()}</li>
                    <!-- BS not support yet -->
                    <li
                        class="dataTypeSelection d-none"
                        only-datatype
                        value="${DataTypes.EU_INTEGER_SEP.name}"
                        data-type="${DataTypes.INTEGER.name}"
                        raw-data-type="${DataTypes.EU_INTEGER_SEP.bs_value}"
                    >${$('#' + DataTypes.EU_INTEGER_SEP.i18nLabelID).text()}</li>
                    <!-- BS not support yet -->
                </ul>
            </div>
            <div class="data-type-selection-box">
                <span class="data-type-selection-title">${$(procModali18n.i18nMultiset).text()}</span>
                <ul>
                    <li class="copyToAllBelow copy-item">${$(procModali18n.copyToAllBelow).text()}</li>
                    <li class="copyToFiltered copy-item">${$(procModali18n.i18nCopyToFiltered).text()}</li>
                </ul>
            </div>
        </div>
    </div>
</div>
`;
    }
}
