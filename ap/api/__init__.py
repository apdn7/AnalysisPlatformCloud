from ap.common.ga import is_app_source_dn


def create_module(app, **kwargs):
    from .aggregate_plot.controllers import api_agp_blueprint
    from .analyze.controllers import api_analyze_module_blueprint
    from .calendar_heatmap.controllers import api_calendar_heatmap_blueprint
    from .categorical_plot.controllers import api_categorical_plot_blueprint
    from .co_occurrence.controllers import api_co_occurrence_blueprint
    from .common.controlllers import api_common_blueprint
    from .external_api.controllers import external_api_v1_blueprint
    from .graphical_lasso.controllers import api_gl_blueprint
    from .heatmap.controllers import api_heatmap_blueprint
    from .multi_scatter_plot.controllers import api_multi_scatter_blueprint
    from .parallel_plot.controllers import api_paracords_blueprint
    from .ridgeline_plot.controllers import api_ridgeline_plot_blueprint
    from .sankey_plot.controllers import api_sankey_plot_blueprint
    from .scatter_plot.controllers import api_scatter_blueprint
    from .setting_module.controllers import api_setting_module_blueprint
    from .table_viewer.controllers import api_table_viewer_blueprint
    from .trace_data.controllers import api_trace_data_blueprint

    app.register_blueprint(api_setting_module_blueprint)
    app.register_blueprint(api_trace_data_blueprint)
    app.register_blueprint(api_table_viewer_blueprint)
    app.register_blueprint(api_scatter_blueprint)
    app.register_blueprint(api_multi_scatter_blueprint)
    app.register_blueprint(api_sankey_plot_blueprint)
    app.register_blueprint(api_co_occurrence_blueprint)
    app.register_blueprint(api_categorical_plot_blueprint)
    app.register_blueprint(api_analyze_module_blueprint)
    app.register_blueprint(api_ridgeline_plot_blueprint)
    app.register_blueprint(api_calendar_heatmap_blueprint)
    app.register_blueprint(api_heatmap_blueprint)
    app.register_blueprint(api_paracords_blueprint)
    app.register_blueprint(api_common_blueprint)
    app.register_blueprint(api_agp_blueprint)
    app.register_blueprint(api_gl_blueprint)
    app.register_blueprint(external_api_v1_blueprint)

    # BRIDGE STATION - Refactor DN & OSS version
    if is_app_source_dn():
        from .name_aggregation_setting.data.controllers import api_nas_data_blueprint
        from .name_aggregation_setting.dept.controllers import api_nas_dept_blueprint
        from .name_aggregation_setting.equip.controllers import api_nas_equip_blueprint
        from .name_aggregation_setting.factory.controllers import api_nas_factory_blueprint
        from .name_aggregation_setting.line.controllers import api_nas_line_blueprint
        from .name_aggregation_setting.part.controllers import api_nas_part_blueprint
        from .name_aggregation_setting.part_type.controllers import api_nas_part_type_blueprint
        from .name_aggregation_setting.plant.controllers import api_nas_plant_blueprint
        from .name_aggregation_setting.process.controllers import api_nas_process_blueprint
        from .name_aggregation_setting.product.controllers import api_nas_product_blueprint
        from .name_aggregation_setting.product_family.controllers import api_nas_product_family_blueprint
        from .name_aggregation_setting.sect.controllers import api_nas_sect_blueprint

        app.register_blueprint(api_nas_factory_blueprint)
        app.register_blueprint(api_nas_plant_blueprint)
        app.register_blueprint(api_nas_product_family_blueprint)
        app.register_blueprint(api_nas_line_blueprint)
        app.register_blueprint(api_nas_dept_blueprint)
        app.register_blueprint(api_nas_sect_blueprint)
        app.register_blueprint(api_nas_equip_blueprint)
        app.register_blueprint(api_nas_product_blueprint)
        app.register_blueprint(api_nas_part_type_blueprint)
        app.register_blueprint(api_nas_part_blueprint)
        app.register_blueprint(api_nas_process_blueprint)
        app.register_blueprint(api_nas_data_blueprint)
