# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: grpc_src/models/master.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x1cgrpc_src/models/master.proto\x12\rmodels.master"\xd8\x01\n\x08MsgMAuto\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x10\n\x08plant_no\x18\x02 \x01(\t\x12\x12\n\ncheck_code\x18\x03 \x01(\t\x12\x11\n\tdata_type\x18\x04 \x01(\x03\x12\x0f\n\x07line_no\x18\x05 \x01(\t\x12\x12\n\nprocess_no\x18\x06 \x01(\t\x12\x10\n\x08\x65quip_no\x18\x07 \x01(\t\x12\x16\n\x0etypicalpart_no\x18\x08 \x01(\t\x12\x10\n\x08\x63hart_no\x18\t \x01(\t\x12\x12\n\nupperlimit\x18\n \x01(\x03\x12\x12\n\nlowerlimit\x18\x0b \x01(\x03"\xd0\x01\n\x12MsgMConfigEquation\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x12\n\neq_type_id\x18\x02 \x01(\x03\x12\r\n\x05var_x\x18\x03 \x01(\x03\x12\r\n\x05var_y\x18\x04 \x01(\x03\x12\r\n\x05var_z\x18\x05 \x01(\x03\x12\r\n\x05\x63oe_a\x18\x06 \x01(\x01\x12\r\n\x05\x63oe_b\x18\x07 \x01(\x01\x12\r\n\x05\x63oe_c\x18\x08 \x01(\x01\x12\r\n\x05par_n\x18\t \x01(\x03\x12\r\n\x05par_k\x18\n \x01(\x03\x12\r\n\x05par_j\x18\x0b \x01(\x03\x12\x13\n\x0breturn_type\x18\x0c \x01(\t"\x96\x01\n\x08MsgMData\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x12\n\nprocess_id\x18\x02 \x01(\x03\x12\x15\n\rdata_group_id\x18\x03 \x01(\x03\x12\x11\n\tdata_type\x18\x04 \x01(\t\x12\x0f\n\x07unit_id\x18\x05 \x01(\x03\x12\x1a\n\x12\x63onfig_equation_id\x18\x06 \x01(\x03\x12\x13\n\x0b\x64\x61ta_factid\x18\x07 \x01(\x03"\xa3\x01\n\rMsgMDataGroup\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x14\n\x0c\x64\x61ta_name_jp\x18\x03 \x01(\t\x12\x14\n\x0c\x64\x61ta_name_en\x18\x07 \x01(\t\x12\x15\n\rdata_name_sys\x18\x04 \x01(\t\x12\x17\n\x0f\x64\x61ta_name_local\x18\x05 \x01(\t\x12\x11\n\tdata_abbr\x18\x06 \x01(\t\x12\x17\n\x0f\x64\x61ta_group_type\x18\x08 \x01(\t"\xf5\x01\n\x0eMsgMDefectMode\x12\x0e\n\x06serial\x18\x01 \x01(\x03\x12\x10\n\x08plant_no\x18\x02 \x01(\t\x12\x0f\n\x07line_no\x18\x03 \x01(\t\x12\x12\n\nprocess_no\x18\x04 \x01(\t\x12\x11\n\tdefect_no\x18\x06 \x01(\t\x12\x11\n\tmode_name\x18\x07 \x01(\t\x12\x13\n\x0b\x64\x65\x66\x65\x63t_name\x18\x08 \x01(\t\x12\x13\n\x0b\x64\x65\x66\x65\x63t_type\x18\t \x01(\x08\x12\x12\n\nstart_date\x18\n \x01(\t\x12\x10\n\x08\x65nd_date\x18\x0b \x01(\t\x12\x13\n\x0b\x65\x64it_userid\x18\x0c \x01(\t\x12\x11\n\tedit_date\x18\r \x01(\t"\xb0\x01\n\x08MsgMDept\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x13\n\x0b\x64\x65pt_factid\x18\x07 \x01(\t\x12\x14\n\x0c\x64\x65pt_name_jp\x18\x02 \x01(\t\x12\x14\n\x0c\x64\x65pt_name_en\x18\n \x01(\t\x12\x15\n\rdept_name_sys\x18\x03 \x01(\t\x12\x17\n\x0f\x64\x65pt_name_local\x18\x04 \x01(\t\x12\x11\n\tdept_abbr\x18\x05 \x01(\t\x12\x14\n\x0c\x64\x65pt_abbr_en\x18\x06 \x01(\t")\n\nMsgMEqType\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0f\n\x07\x65q_type\x18\x02 \x01(\t"k\n\tMsgMEquip\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x16\n\x0e\x65quip_group_id\x18\x02 \x01(\x03\x12\x10\n\x08\x65quip_no\x18\x03 \x01(\t\x12\x12\n\nequip_abbr\x18\x04 \x01(\t\x12\x14\n\x0c\x65quip_factid\x18\x05 \x01(\t"|\n\x0eMsgMEquipGroup\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x15\n\requip_name_jp\x18\x03 \x01(\t\x12\x15\n\requip_name_en\x18\x02 \x01(\t\x12\x16\n\x0e\x65quip_name_sys\x18\x04 \x01(\t\x12\x18\n\x10\x65quip_name_local\x18\x05 \x01(\t"\xda\x01\n\x0bMsgMFactory\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x16\n\x0e\x66\x61\x63tory_factid\x18\x08 \x01(\t\x12\x17\n\x0f\x66\x61\x63tory_name_jp\x18\x02 \x01(\t\x12\x17\n\x0f\x66\x61\x63tory_name_en\x18\n \x01(\t\x12\x18\n\x10\x66\x61\x63tory_name_sys\x18\x03 \x01(\t\x12\x1a\n\x12\x66\x61\x63tory_name_local\x18\x04 \x01(\t\x12\x10\n\x08location\x18\x05 \x01(\t\x12\x14\n\x0c\x66\x61\x63tory_abbr\x18\x06 \x01(\t\x12\x17\n\x0f\x66\x61\x63tory_abbr_en\x18\x07 \x01(\t"\xbc\x01\n\x08MsgMLine\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x10\n\x08plant_id\x18\x02 \x01(\x03\x12\x16\n\x0eprod_family_id\x18\x03 \x01(\x03\x12\x15\n\rline_group_id\x18\x04 \x01(\x03\x12\x13\n\x0bline_factid\x18\t \x01(\t\x12\x0f\n\x07line_no\x18\x05 \x01(\t\x12\x11\n\tline_abbr\x18\x06 \x01(\t\x12\x18\n\x10outsourcing_flag\x18\x07 \x01(\x08\x12\x10\n\x08supplier\x18\x08 \x01(\t"w\n\rMsgMLineGroup\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x14\n\x0cline_name_jp\x18\x02 \x01(\t\x12\x14\n\x0cline_name_en\x18\n \x01(\t\x12\x15\n\rline_name_sys\x18\x03 \x01(\t\x12\x17\n\x0fline_name_local\x18\x04 \x01(\t"\x80\x01\n\x0cMsgMLocation\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x18\n\x10location_name_jp\x18\x02 \x01(\t\x12\x18\n\x10location_name_en\x18\n \x01(\t\x12\x19\n\x11location_name_sys\x18\x03 \x01(\t\x12\x15\n\rlocation_abbr\x18\x04 \x01(\t"t\n\x08MsgMPart\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x15\n\rparts_type_id\x18\x02 \x01(\x03\x12\x13\n\x0bpart_factid\x18\x03 \x01(\t\x12\x0f\n\x07part_no\x18\x04 \x01(\t\x12\x10\n\x08location\x18\x05 \x01(\t\x12\r\n\x05\x64ummy\x18\x06 \x01(\x08"]\n\rMsgMPartsType\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x19\n\x11parts_type_factid\x18\x02 \x01(\t\x12\x12\n\nparts_name\x18\x03 \x01(\t\x12\x11\n\tassy_flag\x18\x04 \x01(\x03"\xcc\x01\n\tMsgMPlant\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x12\n\nfactory_id\x18\x02 \x01(\x03\x12\x14\n\x0cplant_factid\x18\x03 \x01(\t\x12\x15\n\rplant_name_jp\x18\x04 \x01(\t\x12\x15\n\rplant_name_en\x18\n \x01(\t\x12\x16\n\x0eplant_name_sys\x18\x05 \x01(\t\x12\x18\n\x10plant_name_local\x18\x06 \x01(\t\x12\x12\n\nplant_abbr\x18\x07 \x01(\t\x12\x15\n\rplant_abbr_en\x18\x08 \x01(\t"\xc7\x01\n\x0bMsgMProcess\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x16\n\x0eprod_family_id\x18\x02 \x01(\x03\x12\x16\n\x0eprocess_factid\x18\x08 \x01(\t\x12\x17\n\x0fprocess_name_jp\x18\x03 \x01(\t\x12\x17\n\x0fprocess_name_en\x18\x07 \x01(\t\x12\x18\n\x10process_name_sys\x18\x04 \x01(\t\x12\x1a\n\x12process_name_local\x18\x05 \x01(\t\x12\x14\n\x0cprocess_abbr\x18\x06 \x01(\t"\xbe\x01\n\x14MsgMProcessCondition\x12\x0f\n\x07line_no\x18\x02 \x01(\t\x12\x12\n\nprocess_no\x18\x03 \x01(\t\x12\x14\n\x0c\x63ondition_no\x18\x04 \x01(\t\x12\x16\n\x0e\x63ondition_code\x18\x05 \x01(\t\x12\x18\n\x10p_condition_name\x18\x06 \x01(\t\x12\x11\n\tattribute\x18\x07 \x01(\x08\x12\x13\n\x0b\x65\x64it_userid\x18\x1e \x01(\t\x12\x11\n\tedit_date\x18\x1f \x01(\t"\xa0\x01\n\x0bMsgMProduct\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x16\n\x0eprod_family_id\x18\x02 \x01(\x03\x12\x14\n\x0cprod_name_jp\x18\x03 \x01(\t\x12\x14\n\x0cprod_name_en\x18\n \x01(\t\x12\x15\n\rprod_name_sys\x18\x04 \x01(\t\x12\x17\n\x0fprod_name_local\x18\x05 \x01(\t\x12\x11\n\tprod_abbr\x18\x06 \x01(\t"\x8e\x01\n\x11MsgMProductFamily\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x14\n\x0cprod_name_jp\x18\x03 \x01(\t\x12\x14\n\x0cprod_name_en\x18\n \x01(\t\x12\x15\n\rprod_name_sys\x18\x04 \x01(\t\x12\x17\n\x0fprod_name_local\x18\x05 \x01(\t\x12\x11\n\tprod_abbr\x18\x06 \x01(\t"_\n\x0bMsgMQuality\x12\x12\n\nquality_id\x18\x01 \x01(\t\x12\x14\n\x0cquality_name\x18\x02 \x01(\t\x12\x13\n\x0b\x65\x64it_userid\x18\x1e \x01(\t\x12\x11\n\tedit_date\x18\x1f \x01(\t"\xc1\x01\n\x08MsgMSect\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0f\n\x07\x64\x65pt_id\x18\x02 \x01(\x03\x12\x13\n\x0bsect_factid\x18\x08 \x01(\t\x12\x14\n\x0csect_name_jp\x18\x03 \x01(\t\x12\x14\n\x0csect_name_en\x18\n \x01(\t\x12\x15\n\rsect_name_sys\x18\x04 \x01(\t\x12\x17\n\x0fsect_name_local\x18\x05 \x01(\t\x12\x11\n\tsect_abbr\x18\x06 \x01(\t\x12\x14\n\x0csect_abbr_en\x18\x07 \x01(\t"9\n\x10MsgMSubPartGroup\x12\x10\n\x08group_id\x18\x01 \x01(\x03\x12\x13\n\x0bsub_part_id\x18\x02 \x01(\x03"\xa0\x01\n\x08MsgMUnit\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x10\n\x08quantity\x18\x02 \x01(\t\x12\x13\n\x0bquantity_en\x18\x03 \x01(\t\x12\x0c\n\x04unit\x18\x04 \x01(\t\x12\x0c\n\x04type\x18\x05 \x01(\t\x12\x0c\n\x04\x62\x61se\x18\x06 \x01(\x03\x12\x12\n\nconversion\x18\x07 \x01(\x01\x12\x13\n\x0b\x64\x65nominator\x18\x08 \x01(\x01\x12\x0e\n\x06offset\x18\t \x01(\x03"\xb6\x02\n\x18MsgMappingFactoryMachine\x12\x11\n\tt_line_id\x18\x02 \x01(\t\x12\x13\n\x0bt_line_name\x18\x03 \x01(\t\x12\x12\n\nt_equip_id\x18\x04 \x01(\t\x12\x14\n\x0ct_equip_name\x18\x05 \x01(\t\x12\x11\n\tt_dept_id\x18\x06 \x01(\t\x12\x13\n\x0bt_dept_name\x18\x07 \x01(\t\x12\x14\n\x0ct_process_id\x18\x08 \x01(\t\x12\x16\n\x0et_process_name\x18\t \x01(\t\x12\x14\n\x0ct_factory_id\x18\n \x01(\t\x12\x16\n\x0et_factory_name\x18\x0b \x01(\t\x12\x12\n\nt_plant_id\x18\x0c \x01(\t\x12\x14\n\x0ct_plant_name\x18\r \x01(\t\x12\x1a\n\x12\x66\x61\x63tory_machine_id\x18\x0e \x01(\x03"4\n\x0eMsgMappingPart\x12\x11\n\tt_part_no\x18\x02 \x01(\t\x12\x0f\n\x07part_id\x18\x03 \x01(\x03"\x84\x01\n\x15MsgMappingProcessData\x12\x14\n\x0ct_process_id\x18\x02 \x01(\t\x12\x16\n\x0et_process_name\x18\x03 \x01(\t\x12\x14\n\x0ct_quality_id\x18\x04 \x01(\t\x12\x16\n\x0et_quality_name\x18\x05 \x01(\t\x12\x0f\n\x07\x64\x61ta_id\x18\x06 \x01(\x03"z\n\x12MsgRFactoryMachine\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0f\n\x07line_id\x18\x02 \x01(\x03\x12\x12\n\nprocess_id\x18\x03 \x01(\x03\x12\x10\n\x08\x65quip_id\x18\x04 \x01(\x03\x12\x10\n\x08\x65quip_st\x18\x05 \x01(\t\x12\x0f\n\x07sect_id\x18\x06 \x01(\x03"W\n\x0cMsgRProdPart\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0f\n\x07prod_id\x18\x02 \x01(\x03\x12\x0f\n\x07part_id\x18\x03 \x01(\x03\x12\x19\n\x11sub_part_group_id\x18\x04 \x01(\x03"W\n\rMsgSemiMaster\x12\x0e\n\x06\x66\x61\x63tor\x18\x01 \x01(\x03\x12\x0f\n\x07\x64\x61ta_id\x18\x02 \x01(\x03\x12\x12\n\nvalue_text\x18\x03 \x01(\t\x12\x11\n\tvalue_int\x18\x04 \x01(\x03"\x95\x0c\n\x06Insert\x12(\n\x07m_autos\x18\x01 \x03(\x0b\x32\x17.models.master.MsgMAuto\x12=\n\x12m_config_equations\x18\x02 \x03(\x0b\x32!.models.master.MsgMConfigEquation\x12(\n\x07m_datas\x18\x03 \x03(\x0b\x32\x17.models.master.MsgMData\x12\x33\n\rm_data_groups\x18\x04 \x03(\x0b\x32\x1c.models.master.MsgMDataGroup\x12\x35\n\x0em_defect_modes\x18\x05 \x03(\x0b\x32\x1d.models.master.MsgMDefectMode\x12(\n\x07m_depts\x18\x06 \x03(\x0b\x32\x17.models.master.MsgMDept\x12-\n\nm_eq_types\x18\x07 \x03(\x0b\x32\x19.models.master.MsgMEqType\x12*\n\x08m_equips\x18\x08 \x03(\x0b\x32\x18.models.master.MsgMEquip\x12\x35\n\x0em_equip_groups\x18\t \x03(\x0b\x32\x1d.models.master.MsgMEquipGroup\x12.\n\nm_factorys\x18\n \x03(\x0b\x32\x1a.models.master.MsgMFactory\x12(\n\x07m_lines\x18\x0b \x03(\x0b\x32\x17.models.master.MsgMLine\x12\x33\n\rm_line_groups\x18\x0c \x03(\x0b\x32\x1c.models.master.MsgMLineGroup\x12\x30\n\x0bm_locations\x18\r \x03(\x0b\x32\x1b.models.master.MsgMLocation\x12(\n\x07m_parts\x18\x0e \x03(\x0b\x32\x17.models.master.MsgMPart\x12\x33\n\rm_parts_types\x18\x0f \x03(\x0b\x32\x1c.models.master.MsgMPartsType\x12*\n\x08m_plants\x18\x10 \x03(\x0b\x32\x18.models.master.MsgMPlant\x12.\n\nm_processs\x18\x11 \x03(\x0b\x32\x1a.models.master.MsgMProcess\x12\x41\n\x14m_process_conditions\x18\x12 \x03(\x0b\x32#.models.master.MsgMProcessCondition\x12+\n\x07m_prods\x18\x13 \x03(\x0b\x32\x1a.models.master.MsgMProduct\x12\x38\n\x0em_prod_familys\x18\x14 \x03(\x0b\x32 .models.master.MsgMProductFamily\x12.\n\nm_qualitys\x18\x15 \x03(\x0b\x32\x1a.models.master.MsgMQuality\x12(\n\x07m_sects\x18\x16 \x03(\x0b\x32\x17.models.master.MsgMSect\x12:\n\x11m_sub_part_groups\x18\x17 \x03(\x0b\x32\x1f.models.master.MsgMSubPartGroup\x12(\n\x07m_units\x18\x18 \x03(\x0b\x32\x17.models.master.MsgMUnit\x12I\n\x18mapping_factory_machines\x18\x19 \x03(\x0b\x32\'.models.master.MsgMappingFactoryMachine\x12\x34\n\rmapping_parts\x18\x1a \x03(\x0b\x32\x1d.models.master.MsgMappingPart\x12\x43\n\x15mapping_process_datas\x18\x1b \x03(\x0b\x32$.models.master.MsgMappingProcessData\x12=\n\x12r_factory_machines\x18\x1c \x03(\x0b\x32!.models.master.MsgRFactoryMachine\x12\x31\n\x0cr_prod_parts\x18\x1d \x03(\x0b\x32\x1b.models.master.MsgRProdPart\x12\x32\n\x0csemi_masters\x18\x1e \x03(\x0b\x32\x1c.models.master.MsgSemiMaster"\x95\x0c\n\x06Update\x12(\n\x07m_autos\x18\x01 \x03(\x0b\x32\x17.models.master.MsgMAuto\x12=\n\x12m_config_equations\x18\x02 \x03(\x0b\x32!.models.master.MsgMConfigEquation\x12(\n\x07m_datas\x18\x03 \x03(\x0b\x32\x17.models.master.MsgMData\x12\x33\n\rm_data_groups\x18\x04 \x03(\x0b\x32\x1c.models.master.MsgMDataGroup\x12\x35\n\x0em_defect_modes\x18\x05 \x03(\x0b\x32\x1d.models.master.MsgMDefectMode\x12(\n\x07m_depts\x18\x06 \x03(\x0b\x32\x17.models.master.MsgMDept\x12-\n\nm_eq_types\x18\x07 \x03(\x0b\x32\x19.models.master.MsgMEqType\x12*\n\x08m_equips\x18\x08 \x03(\x0b\x32\x18.models.master.MsgMEquip\x12\x35\n\x0em_equip_groups\x18\t \x03(\x0b\x32\x1d.models.master.MsgMEquipGroup\x12.\n\nm_factorys\x18\n \x03(\x0b\x32\x1a.models.master.MsgMFactory\x12(\n\x07m_lines\x18\x0b \x03(\x0b\x32\x17.models.master.MsgMLine\x12\x33\n\rm_line_groups\x18\x0c \x03(\x0b\x32\x1c.models.master.MsgMLineGroup\x12\x30\n\x0bm_locations\x18\r \x03(\x0b\x32\x1b.models.master.MsgMLocation\x12(\n\x07m_parts\x18\x0e \x03(\x0b\x32\x17.models.master.MsgMPart\x12\x33\n\rm_parts_types\x18\x0f \x03(\x0b\x32\x1c.models.master.MsgMPartsType\x12*\n\x08m_plants\x18\x10 \x03(\x0b\x32\x18.models.master.MsgMPlant\x12.\n\nm_processs\x18\x11 \x03(\x0b\x32\x1a.models.master.MsgMProcess\x12\x41\n\x14m_process_conditions\x18\x12 \x03(\x0b\x32#.models.master.MsgMProcessCondition\x12+\n\x07m_prods\x18\x13 \x03(\x0b\x32\x1a.models.master.MsgMProduct\x12\x38\n\x0em_prod_familys\x18\x14 \x03(\x0b\x32 .models.master.MsgMProductFamily\x12.\n\nm_qualitys\x18\x15 \x03(\x0b\x32\x1a.models.master.MsgMQuality\x12(\n\x07m_sects\x18\x16 \x03(\x0b\x32\x17.models.master.MsgMSect\x12:\n\x11m_sub_part_groups\x18\x17 \x03(\x0b\x32\x1f.models.master.MsgMSubPartGroup\x12(\n\x07m_units\x18\x18 \x03(\x0b\x32\x17.models.master.MsgMUnit\x12I\n\x18mapping_factory_machines\x18\x19 \x03(\x0b\x32\'.models.master.MsgMappingFactoryMachine\x12\x34\n\rmapping_parts\x18\x1a \x03(\x0b\x32\x1d.models.master.MsgMappingPart\x12\x43\n\x15mapping_process_datas\x18\x1b \x03(\x0b\x32$.models.master.MsgMappingProcessData\x12=\n\x12r_factory_machines\x18\x1c \x03(\x0b\x32!.models.master.MsgRFactoryMachine\x12\x31\n\x0cr_prod_parts\x18\x1d \x03(\x0b\x32\x1b.models.master.MsgRProdPart\x12\x32\n\x0csemi_masters\x18\x1e \x03(\x0b\x32\x1c.models.master.MsgSemiMaster"\x95\x0c\n\x06\x44\x65lete\x12(\n\x07m_autos\x18\x01 \x03(\x0b\x32\x17.models.master.MsgMAuto\x12=\n\x12m_config_equations\x18\x02 \x03(\x0b\x32!.models.master.MsgMConfigEquation\x12(\n\x07m_datas\x18\x03 \x03(\x0b\x32\x17.models.master.MsgMData\x12\x33\n\rm_data_groups\x18\x04 \x03(\x0b\x32\x1c.models.master.MsgMDataGroup\x12\x35\n\x0em_defect_modes\x18\x05 \x03(\x0b\x32\x1d.models.master.MsgMDefectMode\x12(\n\x07m_depts\x18\x06 \x03(\x0b\x32\x17.models.master.MsgMDept\x12-\n\nm_eq_types\x18\x07 \x03(\x0b\x32\x19.models.master.MsgMEqType\x12*\n\x08m_equips\x18\x08 \x03(\x0b\x32\x18.models.master.MsgMEquip\x12\x35\n\x0em_equip_groups\x18\t \x03(\x0b\x32\x1d.models.master.MsgMEquipGroup\x12.\n\nm_factorys\x18\n \x03(\x0b\x32\x1a.models.master.MsgMFactory\x12(\n\x07m_lines\x18\x0b \x03(\x0b\x32\x17.models.master.MsgMLine\x12\x33\n\rm_line_groups\x18\x0c \x03(\x0b\x32\x1c.models.master.MsgMLineGroup\x12\x30\n\x0bm_locations\x18\r \x03(\x0b\x32\x1b.models.master.MsgMLocation\x12(\n\x07m_parts\x18\x0e \x03(\x0b\x32\x17.models.master.MsgMPart\x12\x33\n\rm_parts_types\x18\x0f \x03(\x0b\x32\x1c.models.master.MsgMPartsType\x12*\n\x08m_plants\x18\x10 \x03(\x0b\x32\x18.models.master.MsgMPlant\x12.\n\nm_processs\x18\x11 \x03(\x0b\x32\x1a.models.master.MsgMProcess\x12\x41\n\x14m_process_conditions\x18\x12 \x03(\x0b\x32#.models.master.MsgMProcessCondition\x12+\n\x07m_prods\x18\x13 \x03(\x0b\x32\x1a.models.master.MsgMProduct\x12\x38\n\x0em_prod_familys\x18\x14 \x03(\x0b\x32 .models.master.MsgMProductFamily\x12.\n\nm_qualitys\x18\x15 \x03(\x0b\x32\x1a.models.master.MsgMQuality\x12(\n\x07m_sects\x18\x16 \x03(\x0b\x32\x17.models.master.MsgMSect\x12:\n\x11m_sub_part_groups\x18\x17 \x03(\x0b\x32\x1f.models.master.MsgMSubPartGroup\x12(\n\x07m_units\x18\x18 \x03(\x0b\x32\x17.models.master.MsgMUnit\x12I\n\x18mapping_factory_machines\x18\x19 \x03(\x0b\x32\'.models.master.MsgMappingFactoryMachine\x12\x34\n\rmapping_parts\x18\x1a \x03(\x0b\x32\x1d.models.master.MsgMappingPart\x12\x43\n\x15mapping_process_datas\x18\x1b \x03(\x0b\x32$.models.master.MsgMappingProcessData\x12=\n\x12r_factory_machines\x18\x1c \x03(\x0b\x32!.models.master.MsgRFactoryMachine\x12\x31\n\x0cr_prod_parts\x18\x1d \x03(\x0b\x32\x1b.models.master.MsgRProdPart\x12\x32\n\x0csemi_masters\x18\x1e \x03(\x0b\x32\x1c.models.master.MsgSemiMasterb\x06proto3'
)


_MSGMAUTO = DESCRIPTOR.message_types_by_name['MsgMAuto']
_MSGMCONFIGEQUATION = DESCRIPTOR.message_types_by_name['MsgMConfigEquation']
_MSGMDATA = DESCRIPTOR.message_types_by_name['MsgMData']
_MSGMDATAGROUP = DESCRIPTOR.message_types_by_name['MsgMDataGroup']
_MSGMDEFECTMODE = DESCRIPTOR.message_types_by_name['MsgMDefectMode']
_MSGMDEPT = DESCRIPTOR.message_types_by_name['MsgMDept']
_MSGMEQTYPE = DESCRIPTOR.message_types_by_name['MsgMEqType']
_MSGMEQUIP = DESCRIPTOR.message_types_by_name['MsgMEquip']
_MSGMEQUIPGROUP = DESCRIPTOR.message_types_by_name['MsgMEquipGroup']
_MSGMFACTORY = DESCRIPTOR.message_types_by_name['MsgMFactory']
_MSGMLINE = DESCRIPTOR.message_types_by_name['MsgMLine']
_MSGMLINEGROUP = DESCRIPTOR.message_types_by_name['MsgMLineGroup']
_MSGMLOCATION = DESCRIPTOR.message_types_by_name['MsgMLocation']
_MSGMPART = DESCRIPTOR.message_types_by_name['MsgMPart']
_MSGMPARTSTYPE = DESCRIPTOR.message_types_by_name['MsgMPartsType']
_MSGMPLANT = DESCRIPTOR.message_types_by_name['MsgMPlant']
_MSGMPROCESS = DESCRIPTOR.message_types_by_name['MsgMProcess']
_MSGMPROCESSCONDITION = DESCRIPTOR.message_types_by_name['MsgMProcessCondition']
_MSGMPRODUCT = DESCRIPTOR.message_types_by_name['MsgMProduct']
_MSGMPRODUCTFAMILY = DESCRIPTOR.message_types_by_name['MsgMProductFamily']
_MSGMQUALITY = DESCRIPTOR.message_types_by_name['MsgMQuality']
_MSGMSECT = DESCRIPTOR.message_types_by_name['MsgMSect']
_MSGMSUBPARTGROUP = DESCRIPTOR.message_types_by_name['MsgMSubPartGroup']
_MSGMUNIT = DESCRIPTOR.message_types_by_name['MsgMUnit']
_MSGMAPPINGFACTORYMACHINE = DESCRIPTOR.message_types_by_name['MsgMappingFactoryMachine']
_MSGMAPPINGPART = DESCRIPTOR.message_types_by_name['MsgMappingPart']
_MSGMAPPINGPROCESSDATA = DESCRIPTOR.message_types_by_name['MsgMappingProcessData']
_MSGRFACTORYMACHINE = DESCRIPTOR.message_types_by_name['MsgRFactoryMachine']
_MSGRPRODPART = DESCRIPTOR.message_types_by_name['MsgRProdPart']
_MSGSEMIMASTER = DESCRIPTOR.message_types_by_name['MsgSemiMaster']
_INSERT = DESCRIPTOR.message_types_by_name['Insert']
_UPDATE = DESCRIPTOR.message_types_by_name['Update']
_DELETE = DESCRIPTOR.message_types_by_name['Delete']
MsgMAuto = _reflection.GeneratedProtocolMessageType(
    'MsgMAuto',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMAUTO,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMAuto)
    },
)
_sym_db.RegisterMessage(MsgMAuto)

MsgMConfigEquation = _reflection.GeneratedProtocolMessageType(
    'MsgMConfigEquation',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMCONFIGEQUATION,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMConfigEquation)
    },
)
_sym_db.RegisterMessage(MsgMConfigEquation)

MsgMData = _reflection.GeneratedProtocolMessageType(
    'MsgMData',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMDATA,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMData)
    },
)
_sym_db.RegisterMessage(MsgMData)

MsgMDataGroup = _reflection.GeneratedProtocolMessageType(
    'MsgMDataGroup',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMDATAGROUP,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMDataGroup)
    },
)
_sym_db.RegisterMessage(MsgMDataGroup)

MsgMDefectMode = _reflection.GeneratedProtocolMessageType(
    'MsgMDefectMode',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMDEFECTMODE,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMDefectMode)
    },
)
_sym_db.RegisterMessage(MsgMDefectMode)

MsgMDept = _reflection.GeneratedProtocolMessageType(
    'MsgMDept',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMDEPT,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMDept)
    },
)
_sym_db.RegisterMessage(MsgMDept)

MsgMEqType = _reflection.GeneratedProtocolMessageType(
    'MsgMEqType',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMEQTYPE,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMEqType)
    },
)
_sym_db.RegisterMessage(MsgMEqType)

MsgMEquip = _reflection.GeneratedProtocolMessageType(
    'MsgMEquip',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMEQUIP,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMEquip)
    },
)
_sym_db.RegisterMessage(MsgMEquip)

MsgMEquipGroup = _reflection.GeneratedProtocolMessageType(
    'MsgMEquipGroup',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMEQUIPGROUP,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMEquipGroup)
    },
)
_sym_db.RegisterMessage(MsgMEquipGroup)

MsgMFactory = _reflection.GeneratedProtocolMessageType(
    'MsgMFactory',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMFACTORY,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMFactory)
    },
)
_sym_db.RegisterMessage(MsgMFactory)

MsgMLine = _reflection.GeneratedProtocolMessageType(
    'MsgMLine',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMLINE,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMLine)
    },
)
_sym_db.RegisterMessage(MsgMLine)

MsgMLineGroup = _reflection.GeneratedProtocolMessageType(
    'MsgMLineGroup',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMLINEGROUP,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMLineGroup)
    },
)
_sym_db.RegisterMessage(MsgMLineGroup)

MsgMLocation = _reflection.GeneratedProtocolMessageType(
    'MsgMLocation',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMLOCATION,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMLocation)
    },
)
_sym_db.RegisterMessage(MsgMLocation)

MsgMPart = _reflection.GeneratedProtocolMessageType(
    'MsgMPart',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMPART,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMPart)
    },
)
_sym_db.RegisterMessage(MsgMPart)

MsgMPartsType = _reflection.GeneratedProtocolMessageType(
    'MsgMPartsType',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMPARTSTYPE,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMPartsType)
    },
)
_sym_db.RegisterMessage(MsgMPartsType)

MsgMPlant = _reflection.GeneratedProtocolMessageType(
    'MsgMPlant',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMPLANT,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMPlant)
    },
)
_sym_db.RegisterMessage(MsgMPlant)

MsgMProcess = _reflection.GeneratedProtocolMessageType(
    'MsgMProcess',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMPROCESS,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMProcess)
    },
)
_sym_db.RegisterMessage(MsgMProcess)

MsgMProcessCondition = _reflection.GeneratedProtocolMessageType(
    'MsgMProcessCondition',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMPROCESSCONDITION,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMProcessCondition)
    },
)
_sym_db.RegisterMessage(MsgMProcessCondition)

MsgMProduct = _reflection.GeneratedProtocolMessageType(
    'MsgMProduct',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMPRODUCT,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMProduct)
    },
)
_sym_db.RegisterMessage(MsgMProduct)

MsgMProductFamily = _reflection.GeneratedProtocolMessageType(
    'MsgMProductFamily',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMPRODUCTFAMILY,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMProductFamily)
    },
)
_sym_db.RegisterMessage(MsgMProductFamily)

MsgMQuality = _reflection.GeneratedProtocolMessageType(
    'MsgMQuality',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMQUALITY,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMQuality)
    },
)
_sym_db.RegisterMessage(MsgMQuality)

MsgMSect = _reflection.GeneratedProtocolMessageType(
    'MsgMSect',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMSECT,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMSect)
    },
)
_sym_db.RegisterMessage(MsgMSect)

MsgMSubPartGroup = _reflection.GeneratedProtocolMessageType(
    'MsgMSubPartGroup',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMSUBPARTGROUP,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMSubPartGroup)
    },
)
_sym_db.RegisterMessage(MsgMSubPartGroup)

MsgMUnit = _reflection.GeneratedProtocolMessageType(
    'MsgMUnit',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMUNIT,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMUnit)
    },
)
_sym_db.RegisterMessage(MsgMUnit)

MsgMappingFactoryMachine = _reflection.GeneratedProtocolMessageType(
    'MsgMappingFactoryMachine',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMAPPINGFACTORYMACHINE,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMappingFactoryMachine)
    },
)
_sym_db.RegisterMessage(MsgMappingFactoryMachine)

MsgMappingPart = _reflection.GeneratedProtocolMessageType(
    'MsgMappingPart',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMAPPINGPART,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMappingPart)
    },
)
_sym_db.RegisterMessage(MsgMappingPart)

MsgMappingProcessData = _reflection.GeneratedProtocolMessageType(
    'MsgMappingProcessData',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGMAPPINGPROCESSDATA,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgMappingProcessData)
    },
)
_sym_db.RegisterMessage(MsgMappingProcessData)

MsgRFactoryMachine = _reflection.GeneratedProtocolMessageType(
    'MsgRFactoryMachine',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGRFACTORYMACHINE,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgRFactoryMachine)
    },
)
_sym_db.RegisterMessage(MsgRFactoryMachine)

MsgRProdPart = _reflection.GeneratedProtocolMessageType(
    'MsgRProdPart',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGRPRODPART,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgRProdPart)
    },
)
_sym_db.RegisterMessage(MsgRProdPart)

MsgSemiMaster = _reflection.GeneratedProtocolMessageType(
    'MsgSemiMaster',
    (_message.Message,),
    {
        'DESCRIPTOR': _MSGSEMIMASTER,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.MsgSemiMaster)
    },
)
_sym_db.RegisterMessage(MsgSemiMaster)

Insert = _reflection.GeneratedProtocolMessageType(
    'Insert',
    (_message.Message,),
    {
        'DESCRIPTOR': _INSERT,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.Insert)
    },
)
_sym_db.RegisterMessage(Insert)

Update = _reflection.GeneratedProtocolMessageType(
    'Update',
    (_message.Message,),
    {
        'DESCRIPTOR': _UPDATE,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.Update)
    },
)
_sym_db.RegisterMessage(Update)

Delete = _reflection.GeneratedProtocolMessageType(
    'Delete',
    (_message.Message,),
    {
        'DESCRIPTOR': _DELETE,
        '__module__': 'grpc_src.models.master_pb2'
        # @@protoc_insertion_point(class_scope:models.master.Delete)
    },
)
_sym_db.RegisterMessage(Delete)

if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _MSGMAUTO._serialized_start = 48
    _MSGMAUTO._serialized_end = 264
    _MSGMCONFIGEQUATION._serialized_start = 267
    _MSGMCONFIGEQUATION._serialized_end = 475
    _MSGMDATA._serialized_start = 478
    _MSGMDATA._serialized_end = 628
    _MSGMDATAGROUP._serialized_start = 631
    _MSGMDATAGROUP._serialized_end = 794
    _MSGMDEFECTMODE._serialized_start = 797
    _MSGMDEFECTMODE._serialized_end = 1042
    _MSGMDEPT._serialized_start = 1045
    _MSGMDEPT._serialized_end = 1221
    _MSGMEQTYPE._serialized_start = 1223
    _MSGMEQTYPE._serialized_end = 1264
    _MSGMEQUIP._serialized_start = 1266
    _MSGMEQUIP._serialized_end = 1373
    _MSGMEQUIPGROUP._serialized_start = 1375
    _MSGMEQUIPGROUP._serialized_end = 1499
    _MSGMFACTORY._serialized_start = 1502
    _MSGMFACTORY._serialized_end = 1720
    _MSGMLINE._serialized_start = 1723
    _MSGMLINE._serialized_end = 1911
    _MSGMLINEGROUP._serialized_start = 1913
    _MSGMLINEGROUP._serialized_end = 2032
    _MSGMLOCATION._serialized_start = 2035
    _MSGMLOCATION._serialized_end = 2163
    _MSGMPART._serialized_start = 2165
    _MSGMPART._serialized_end = 2281
    _MSGMPARTSTYPE._serialized_start = 2283
    _MSGMPARTSTYPE._serialized_end = 2376
    _MSGMPLANT._serialized_start = 2379
    _MSGMPLANT._serialized_end = 2583
    _MSGMPROCESS._serialized_start = 2586
    _MSGMPROCESS._serialized_end = 2785
    _MSGMPROCESSCONDITION._serialized_start = 2788
    _MSGMPROCESSCONDITION._serialized_end = 2978
    _MSGMPRODUCT._serialized_start = 2981
    _MSGMPRODUCT._serialized_end = 3141
    _MSGMPRODUCTFAMILY._serialized_start = 3144
    _MSGMPRODUCTFAMILY._serialized_end = 3286
    _MSGMQUALITY._serialized_start = 3288
    _MSGMQUALITY._serialized_end = 3383
    _MSGMSECT._serialized_start = 3386
    _MSGMSECT._serialized_end = 3579
    _MSGMSUBPARTGROUP._serialized_start = 3581
    _MSGMSUBPARTGROUP._serialized_end = 3638
    _MSGMUNIT._serialized_start = 3641
    _MSGMUNIT._serialized_end = 3801
    _MSGMAPPINGFACTORYMACHINE._serialized_start = 3804
    _MSGMAPPINGFACTORYMACHINE._serialized_end = 4114
    _MSGMAPPINGPART._serialized_start = 4116
    _MSGMAPPINGPART._serialized_end = 4168
    _MSGMAPPINGPROCESSDATA._serialized_start = 4171
    _MSGMAPPINGPROCESSDATA._serialized_end = 4303
    _MSGRFACTORYMACHINE._serialized_start = 4305
    _MSGRFACTORYMACHINE._serialized_end = 4427
    _MSGRPRODPART._serialized_start = 4429
    _MSGRPRODPART._serialized_end = 4516
    _MSGSEMIMASTER._serialized_start = 4518
    _MSGSEMIMASTER._serialized_end = 4605
    _INSERT._serialized_start = 4608
    _INSERT._serialized_end = 6165
    _UPDATE._serialized_start = 6168
    _UPDATE._serialized_end = 7725
    _DELETE._serialized_start = 7728
    _DELETE._serialized_end = 9285
# @@protoc_insertion_point(module_scope)
