/**
 * @file Contains helper functions that serve for data type dropdown.
 * @author Pham Minh Hoang <hoangpm6@fpt.com>
 * @author Tran Thi Kim Tuyen <tuyenttk5@fpt.com>
 */

/**
 * Class contains all helper functions that serves for data type dropdown menu control
 */
class DataTypeDropdown_Helper extends DataTypeDropdown_Constant {
    /**
     * Initialize
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     */
    static init(dataTypeDropdownElement) {
        const $dataTypeDropdownElement = $(dataTypeDropdownElement);
        $dataTypeDropdownElement.find('ul li').removeClass('active');

        const showValueOption = this.getShowValueElement(dataTypeDropdownElement);
        const rawDataType = showValueOption.dataset.rawDataType;
        let attrKey = showValueOption.dataset.attrKey;
        attrKey = attrKey === 'is_serial_no' ? '' : attrKey;
        const isRegisteredCol = showValueOption.getAttribute('is-registered-col') === 'true';
        const isBigInt = showValueOption.getAttribute('is-big-int') === 'true';
        const selectOption = this.getOptionByAttrKey(
            dataTypeDropdownElement,
            rawDataType,
            attrKey,
        );
        selectOption.addClass('active');

        this.setValueToShowValueElement(
            dataTypeDropdownElement,
            rawDataType,
            selectOption.text(),
            attrKey,
        );
        this.setDefaultNameAndDisableInput(dataTypeDropdownElement, attrKey);

        this.disableOtherDataType(
            dataTypeDropdownElement,
            isRegisteredCol || isBigInt,
            rawDataType,
            attrKey,
        );

        // disable copy function if allow select one item
        this.disableCopyItem(dataTypeDropdownElement, attrKey);
        if (currentProcess?.is_show_file_name != null) {
            // data of process imported
            this.disableDatetimeMainItem(dataTypeDropdownElement);
        }
    }

    /**
     * Check data type is allow to edit format or not
     * @param {string} dataType - a data type string
     * @return {boolean} - true: allow format, false: not allow to edit format
     */
    static isDataTypeAllowFormat = (dataType) => {
        return this.AllowFormatingDataType.includes(dataType);
    };

    /**
     * Hide all data type dropdown on UI
     */
    static hideAllDropdownMenu() {
        $('.data-type-selection').hide();
    }

    /**
     * Get Show Value Element
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @return {HTMLElement}
     */
    static getShowValueElement(dataTypeDropdownElement) {
        return dataTypeDropdownElement.querySelector(`span[name="${this.ElementNames.dataType}"]`);
    }

    /**
     * Get Option By Attribute Key
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {string} value - value of option
     * @param {string} attrKey - attrKey of option
     * @return {jQuery}
     */
    static getOptionByAttrKey(dataTypeDropdownElement, value, attrKey) {
        return $(dataTypeDropdownElement)
            .find(`ul li[value=${value}]${attrKey ? `[${attrKey}]` : '[only-datatype]'}`)
            .first();
    }

    /**
     * Set Value To Show Value Element
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {string} value - value of option
     * @param {string} text - text want to set
     * @param {string} attrKey - attrKey of option
     */
    static setValueToShowValueElement(
        dataTypeDropdownElement,
        value,
        text,
        attrKey,
    ) {
        const showValueEl = this.getShowValueElement(dataTypeDropdownElement);

        if (text) {
            showValueEl.textContent = text;
        }
        for (const attr of DataTypeAttrs) {
            showValueEl.removeAttribute(attr);
        }
        showValueEl.removeAttribute('column_type');
        showValueEl.removeAttribute('data-attr-key');

        if (attrKey) {
            showValueEl.setAttribute(attrKey, 'true');
            showValueEl.setAttribute('data-attr-key', attrKey);
            showValueEl.setAttribute(
                'column_type',
                masterDataGroup[mappingDataGroupType[attrKey]],
            );
        }
        showValueEl.setAttribute('value', value);
        showValueEl.setAttribute('data-raw-data-type', value);
    }

    /**
     * Set Default Name And Disable Input
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {string} attrKey - attrKey of option
     */
    static setDefaultNameAndDisableInput(dataTypeDropdownElement, attrKey = '') {
        const $tr = $(dataTypeDropdownElement.closest('tr'));
        const $systemInput = /** @type jQuery<HTMLInputElement> */ $tr.find(
            `input[name=${this.ElementNames.systemName}]`,
        );
        const $japaneseNameInput = /** @type jQuery<HTMLInputElement> */ $tr.find(
            `input[name=${this.ElementNames.japaneseName}]`,
        );
        const $localNameInput = /** @type jQuery<HTMLInputElement> */ $tr.find(
            `input[name=${this.ElementNames.localName}]`,
        );
        const oldValSystem = $systemInput.attr('old-value');
        const oldValJa = $japaneseNameInput.attr('old-value');
        if (fixedNameAttrs.includes(attrKey)) {
            // set default value to system and input
            if (!oldValSystem || !oldValJa) {
                $systemInput.attr('old-value', $systemInput.val());
                $japaneseNameInput.attr('old-value', $japaneseNameInput.val());
            }
            $systemInput.val(fixedName[attrKey].system).prop('disabled', true);
            $japaneseNameInput
                .val(fixedName[attrKey].japanese)
                .prop('disabled', true);
            if (!$localNameInput.val()) {
                // fill value of local name only blank, do not disabled.
                $localNameInput.val(fixedName[attrKey].system);
            }
        } else {
            if (oldValSystem && oldValJa) {
                $systemInput.val(oldValSystem);
                $japaneseNameInput.val(oldValJa);
                $localNameInput.val('');
            }

            $systemInput.prop('disabled', false);
            $japaneseNameInput.prop('disabled', false);
        }
    }

    /**
     * Disable Other DataType
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {boolean} isRegisteredCol - isRegisteredCol
     * @param {string} dataType - data type want to disable
     * @param {string} attrKey - attrKey of option
     */
    static disableOtherDataType(dataTypeDropdownElement, isRegisteredCol = false, dataType, attrKey) {
        if (!isRegisteredCol) return;
        if (this.UnableToReselectAttrs.includes(attrKey)) {
            // disable all option
            $(dataTypeDropdownElement)
                .find(`ul li.dataTypeSelection:not([${attrKey}])`)
                .attr('disabled', true);
        } else {
            // select all other data type option -> add disabled
            $(dataTypeDropdownElement)
                .find(`ul li.dataTypeSelection[data-type!=${dataType}]`)
                .attr('disabled', true);
        }
    }

    /**
     * Disable Copy Item
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {string} attrKey
     */
    static disableCopyItem(dataTypeDropdownElement, attrKey) {
        // disable copy function if allow select one item
        if (this.AllowSelectOneAttrs.includes(attrKey)) {
            $(dataTypeDropdownElement)
                .find(`ul li.copy-item`)
                .attr('disabled', true);
        } else {
            $(dataTypeDropdownElement)
                .find(`ul li.copy-item`)
                .attr('disabled', false);
        }
    }

    /**
     * Disable Datetime Main Item
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     */
    static disableDatetimeMainItem(dataTypeDropdownElement) {
        // disable copy function if allow select one item
        $(dataTypeDropdownElement)
            .find(`ul li.dataTypeSelection[is_get_date]`)
            .attr('disabled', true);
        $(dataTypeDropdownElement)
            .find(`ul li.dataTypeSelection[is_main_date]`)
            .attr('disabled', true);
        $(dataTypeDropdownElement)
            .find(`ul li.dataTypeSelection[is_main_time]`)
            .attr('disabled', true);
    }

    /**
     * onClick Data Type
     * @param {Event|HTMLLIElement} event
     */
    static onClickDataType(event) {
        const currentTarget = /** @type HTMLLIElement */ event.currentTarget || event;
        const dataTypeDropdownElement = /** @type HTMLDivElement */ currentTarget.closest('div.config-data-type-dropdown');
        const attrKey = this.getAttrOfDataTypeItem(currentTarget);
        const value = currentTarget.getAttribute('value');

        this.changeDataType(
            dataTypeDropdownElement,
            value,
            currentTarget.textContent,
            attrKey,
            currentTarget,
        );
    }

    /**
     * Get Attribute Of DataType Item
     * @param {HTMLElement | Event} event
     * @return {string|string}
     */
    static getAttrOfDataTypeItem(event) {
        const target = /** @type HTMLElement */ event.currentTarget || event;
        const attrs = target
            .getAttributeNames()
            .filter((v) => this.ColumnTypeAttrs.includes(v));
        return attrs.length ? attrs[0] : '';
    }

    /**
     * Change DataType
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {string} value
     * @param {string} text
     * @param {string} attrKey
     * @param {HTMLElement} el
     */
    static changeDataType(dataTypeDropdownElement, value, text, attrKey , el = null) {
        this.setValueToShowValueElement(dataTypeDropdownElement, value, text, attrKey);
        this.setDefaultNameAndDisableInput(dataTypeDropdownElement, attrKey);

        // get current datatype
        const beforeDataTypeEle = dataTypeDropdownElement.querySelector(`ul li.active`);
        const beforeAttrKey = beforeDataTypeEle
            ? this.getAttrOfDataTypeItem(beforeDataTypeEle)
            : '';

        $(dataTypeDropdownElement)
            .find(`ul li`)
            .removeClass('active');

        this.getOptionByAttrKey(dataTypeDropdownElement, value, attrKey)
            .addClass('active');

        if (this.AllowSelectOneAttrs.includes(attrKey)) {
            // remove attr of others
            this.resetOtherMainAttrKey(dataTypeDropdownElement, attrKey);
        }

        // disable data type column not input format
        this.enableDisableFormatText(dataTypeDropdownElement, value);

        // disable copy function if allow select one item
        this.disableCopyItem(dataTypeDropdownElement, attrKey);

        if (el) {
            this.parseDataType(dataTypeDropdownElement, el);
        }

        this.setColumnTypeForMainDateMainTime(dataTypeDropdownElement, attrKey);

        ProcessConfigSection.handleMainDateAndMainTime(dataTypeDropdownElement, attrKey, beforeAttrKey);
    }

    /**
     * Change to normal data type for the another columns have same data type with main attribute key
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {string} attrKey - main attribute key
     */
    static resetOtherMainAttrKey(dataTypeDropdownElement, attrKey) {
        // Find same data type element from another columns
        const sameDataTypeElements = /** @type HTMLSpanElement[] */ [];
        [
            ...dataTypeDropdownElement
                .closest('tbody')
                .querySelectorAll('div.config-data-type-dropdown')
        ]
            .forEach(dropdownElement => {
                const sameDataTypeElement = dropdownElement.querySelector(`[name=dataType][${attrKey}]`);
                if (
                    dropdownElement !== dataTypeDropdownElement
                    && sameDataTypeElement != null
                ) {
                    sameDataTypeElements.push(sameDataTypeElement);
                }
            });

        if (!sameDataTypeElements.length) return;

        // Change to normal data type for another columns have same data type with main attribute key
        sameDataTypeElements.forEach((el) => this.changeToNormalDataType(el));
    }

    /**
     * Change to normal data type for another columns have same data type with main attribute key
     * @param {HTMLSpanElement} el
     */
    static changeToNormalDataType(el) {
        const anotherDataTypeDropdownElement = el.closest('div.config-data-type-dropdown');
        const dataType = el.dataset.rawDataType;
        this.init(anotherDataTypeDropdownElement);
        $(anotherDataTypeDropdownElement)
            .find(`li[raw-data-type=${dataType}][only-datatype]`)
            .trigger('click');
    }

    /**
     * Enable Disable Format Text
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {string} rawDataType - raw data type
     */
    static enableDisableFormatText(dataTypeDropdownElement, rawDataType = '') {
        const $tr = $(dataTypeDropdownElement).closest('tr');
        const isAllowFormat = this.isDataTypeAllowFormat(rawDataType);
        const $inputFormat = $tr.find(`input[name=${procModalElements.format}]`);
        const inputFormatValue = $inputFormat.val();
        if (isAllowFormat) {
            if (inputFormatValue == null || inputFormatValue === '') {
                $inputFormat.val($inputFormat[0]?.previousValue ?? '');
            }
        } else {
            if (!(inputFormatValue == null || inputFormatValue === '')) {
                $inputFormat.previousValue = inputFormatValue;
            }
            $inputFormat.val('');
        }
        $inputFormat.prop('disabled', !isAllowFormat);
    }

    /**
     * Set Column Type For Main Date Main Time
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {string} attrKey
     */
    static setColumnTypeForMainDateMainTime(dataTypeDropdownElement, attrKey) {
        const isMainDate = 'is_main_date' === attrKey;
        const isMainTime = 'is_main_time' === attrKey;
        const targetRow = dataTypeDropdownElement.closest('tr');
        const checkboxColumn = targetRow.querySelector(
            'td.column-raw-name input[type="checkbox"]:first-child',
        );

        if (!isMainDate && !isMainTime) {
            const originColumnType =
                checkboxColumn.getAttribute('origin-column-type');
            if (originColumnType != null && originColumnType !== '') {
                checkboxColumn.dataset['column_type'] =
                    checkboxColumn.getAttribute('origin-column-type');
            } else {
                // do nothing
            }
        } else {
            checkboxColumn.setAttribute(
                'origin-column-type',
                checkboxColumn.dataset.column_type ?? '',
            );
            checkboxColumn.dataset.column_type = isMainDate
                ? masterDataGroup.MAIN_DATE
                : masterDataGroup.MAIN_TIME;
            checkboxColumn.dataset.is_get_date = false;
        }
    }

    /**
     * Parse DataType
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @param {HTMLElement} ele
     */
    static parseDataType(dataTypeDropdownElement, ele) {
        parseDataType_New(ele, undefined, dataTypeDropdownElement);
    }

    /**
     * onFocus DataType
     * @param {Event} e
     */
    static onFocusDataType(e) {
        const element = /** @type HTMLLIElement */ e.currentTarget
        element.previousValue = element.value;
    }

    /**
     * Handle Copy To All Below
     * @param {Event} event
     */
    static handleCopyToAllBelow(event) {
        const currentTarget = /** @type HTMLLIElement */ event.currentTarget || event;
        const dataTypeDropdownElement = /** @type HTMLDivElement */ currentTarget.closest('div.config-data-type-dropdown');
        const [value, attrKey, showValueEl] =
            this.getDataOfSelectedOption(dataTypeDropdownElement);
        const optionEl = this.getOptionByAttrKey(
            dataTypeDropdownElement,
            value,
            attrKey,
        );
        const nextRows = [...$(showValueEl.closest('tr')).nextAll()];
        nextRows.forEach((row) => {
            const isMasterCol = row.getAttribute('is-master-col') === 'true';
            if (isMasterCol) {
                return;
            }

            const dataTypeDropdownElement = (
                /** @type HTMLDivElement */
                row.querySelector('div.config-data-type-dropdown')
            );
            this.changeDataType(
                dataTypeDropdownElement,
                value,
                optionEl.text(),
                attrKey,
                this.getOptionByAttrKey(dataTypeDropdownElement, value, attrKey)[0],
            );
        });
    }

    /**
     * Get Data Of Selected Option
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     * @return {(string|string|string|HTMLElement)[]}
     */
    static getDataOfSelectedOption(dataTypeDropdownElement) {
        const showValueEl = this.getShowValueElement(dataTypeDropdownElement);
        const attrKey = this.getAttrOfDataTypeItem(showValueEl);
        const dataType = showValueEl.dataset.rawDataType;
        return [dataType, attrKey, showValueEl];
    }

    /**
     * Handle Copy To Filtered
     * @param {Event} event
     */
    static handleCopyToFiltered(event) {
        const currentTarget = /** @type HTMLLIElement */ event.currentTarget || event;
        const dataTypeDropdownElement = /** @type HTMLDivElement */ currentTarget.closest('div.config-data-type-dropdown');
        const [value, attrKey,] =
            this.getDataOfSelectedOption(dataTypeDropdownElement);
        const optionEl = this.getOptionByAttrKey(
            dataTypeDropdownElement,
            value,
            attrKey,
        );
        const filterRows = [
            ...$(dataTypeDropdownElement)
                .closest('table')
                .find('tbody tr:not(.gray):visible'),
        ];
        filterRows.forEach((row) => {
            const isMasterCol = row.getAttribute('is-master-col') === 'true';
            if (isMasterCol) {
                return;
            }
            const dataTypeDropdownElement = (
                /** @type HTMLDivElement */
                row.querySelector('div.config-data-type-dropdown')
            );
            this.changeDataType(
                dataTypeDropdownElement,
                value,
                optionEl.text(),
                attrKey,
                this.getOptionByAttrKey(dataTypeDropdownElement, value, attrKey)[0],
            );
        });
    }

    /**
     * Set Value For Items
     * @param {HTMLDivElement} dataTypeDropdownElement - an HTML object of dropdown
     */
    static setValueForItems(dataTypeDropdownElement) {
        const aElements = (
            /** @type NodeListOf<HTMLSpanElement> */
            dataTypeDropdownElement.querySelectorAll('button > span')
        );
        aElements.forEach(aElement => {
            aElement.value = aElement.dataset.value;
            aElement.previousValue = aElement.value;
        });
    }

    /**
     * Convert Column Type To Attr Key
     * @public
     * @param {number} columnType - column type
     * @return {ColumnTypeInfo} - column attribute dict
     */
    static convertColumnTypeToAttrKey(columnType = 99)  {
        const col = {};
        switch (columnType) {
            case this.DataGroupType['MAIN_SERIAL']:
                col.is_serial_no = false;
                col.is_main_serial_no = true;
                return col;
            case this.DataGroupType['SERIAL']:
                col.is_serial_no = true;
                col.is_main_serial_no = false;
                return col;
            case this.DataGroupType['LINE_NAME']:
                col.is_line_name = true;
                return col;
            case this.DataGroupType['LINE_NO']:
                col.is_line_no = true;
                return col;
            case this.DataGroupType['EQ_NAME']:
                col.is_eq_name = true;
                return col;
            case this.DataGroupType['EQ_NO']:
                col.is_eq_no = true;
                return col;
            case this.DataGroupType['PART_NAME']:
                col.is_part_name = true;
                return col;
            case this.DataGroupType['PART_NO']:
                col.is_part_no = true;
                return col;
            case this.DataGroupType['ST_NO']:
                col.is_st_no = true;
                return col;
            case this.DataGroupType['INT_CATE']:
                col.is_int_cat = true;
                return col;
            case this.DataGroupType['MAIN_DATE']:
                col.is_main_date = true;
                return col;
            case this.DataGroupType['MAIN_TIME']:
                col.is_main_time = true;
                return col;
            default:
                return col;
        }
    }
}
