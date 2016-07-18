from labqar.tasks import *


# INSERIR REDE
def adicionar_rede(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'Rede\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute("select red_id, red_descricao from rede")
    for row in cursor.fetchall():
        if (row.red_id > last_entry_id):
            inserir_rede.apply_async((row.red_id, row.red_descricao), retry=True, retry_policy={
                'max_retries': 0,  # retry until sent
                'interval_start': 60,  # start retrying one minute later
                'interval_step': 30,  # wait 30 seconds before retrying again
                'interval_max': 180,
            })
            last_entry_id = row.red_id

    cursor.execute('UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'Rede\';')
    cursor.commit()


# INSERIR MAPA
def adicionar_mapa(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'Mapa\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute("select map_id, map_nome, map_ficheiro from mapa")
    for row in cursor.fetchall():
        if (row.map_id > last_entry_id):
            inserir_mapa.apply_async((row.map_id, row.map_nome, row.map_ficheiro), retry=True, retry_policy={
                'max_retries': 0,  # retry until sent
                'interval_start': 60,  # start retrying one minute later
                'interval_step': 30,  # wait 30 seconds before retrying again
                'interval_max': 180,
            })
            last_entry_id = row.map_id

    cursor.execute('UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'Mapa\';')
    cursor.commit()


# INSERIR ESTACAO
def adicionar_estacao(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'Estacao\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute(
        "select est_id, map_id, red_id, est_nome, est_abreviatura, est_codigo, est_instituicao, est_pos_x, est_pos_y, est_local, est_data_instal, est_tipo_ligacao, "
        "est_telefone, est_central_telef, est_indicativo, est_ultima_revisao, est_foto, est_obs, est_alarme_a, est_tel_alarme_a, est_alarme_b, est_tel_alarme_b, "
        "est_alarme_c, est_tel_alarme_c, est_num_tent, est_atraso_tent, est_p_aq_normal, est_p_aq_alarme, est_perc_validacao, est_num_canais_activ, est_mascara, "
        "est_data_validacao, est_comunicacao, est_modem_init, est_modem_dial, est_template, est_relogio_sinc, est_autonomia, est_meteo_est_id, est_meteo_vel_id, "
        "est_meteo_dir_id, est_meteo_used, est_auto_validacao, cam_id, est_porta_serie, est_tipo, est_activa, est_data_ult_act from estacao")
    for row in cursor.fetchall():
        if (row.est_id > last_entry_id):
            if (row.est_data_instal != None):
                row.est_data_instal = row.est_data_instal.isoformat()
            else:
                row.est_data_instal = ""
            if (row.est_ultima_revisao != None):
                row.est_ultima_revisao = row.est_ultima_revisao.isoformat()
            else:
                row.est_ultima_revisao = ""
            if (row.est_data_ult_act != None):
                row.est_data_ult_act = row.est_data_ult_act.isoformat()
            else:
                row.est_data_ult_act = ""
            if (row.est_data_validacao != None):
                row.est_data_validacao = row.est_data_validacao.isoformat()
            else:
                row.est_data_validacao = ""
            inserir_estacao.apply_async((row.est_id, row.map_id, row.red_id, row.est_nome, row.est_abreviatura, row.est_codigo,
                                  row.est_instituicao, row.est_pos_x, row.est_pos_y, row.est_local,
                                  str(row.est_data_instal), row.est_tipo_ligacao, row.est_telefone,
                                  row.est_central_telef, row.est_indicativo, row.est_ultima_revisao, None, row.est_obs,
                                  row.est_alarme_a, row.est_tel_alarme_a, row.est_alarme_b, row.est_tel_alarme_b,
                                  row.est_alarme_c, row.est_tel_alarme_c, row.est_num_tent,
                                  row.est_atraso_tent, row.est_p_aq_normal, row.est_p_aq_alarme, row.est_perc_validacao,
                                  row.est_num_canais_activ, row.est_mascara, row.est_data_validacao,
                                  row.est_comunicacao, row.est_modem_init, row.est_modem_dial, row.est_template,
                                  row.est_relogio_sinc, row.est_autonomia, row.est_meteo_est_id,
                                  row.est_meteo_vel_id, row.est_meteo_dir_id, row.est_meteo_used,
                                  row.est_auto_validacao, row.cam_id, row.est_porta_serie, row.est_tipo, row.est_activa,
                                  row.est_data_ult_act), retry=True, retry_policy={
                    'max_retries': 0,  # retry until sent
                    'interval_start': 60,  # start retrying one minute later
                    'interval_step': 30,  # wait 30 seconds before retrying again
                    'interval_max': 180,
                })
            last_entry_id = row.est_id

    cursor.execute('UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'Estacao\';')
    cursor.commit()


# INSERIR DADOS
def adicionar_dados(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'Dados\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute(
        "select dad_id, est_id, cam_id, dad_data, dad_1med, dad_1stat, dad_2med, dad_2stat, dad_3med, dad_3stat, dad_4med, dad_4stat, dad_5med, dad_5stat, dad_6med, dad_6stat,"
        " dad_7med, dad_7stat, dad_8med, dad_8stat, dad_9med, dad_9stat, dad_10med, dad_10stat, dad_11med, dad_11stat, dad_12med, dad_12stat, dad_13med, dad_13stat, "
        "dad_14med, dad_14stat, dad_15med, dad_15stat, dad_16med, dad_16stat, dad_17med, dad_17stat, dad_18med, dad_18stat, dad_19med, dad_19stat,dad_20med, dad_20stat, "
        "dad_p_aquisicao from dados")
    for row in cursor.fetchall():
        if (row.dad_id > last_entry_id):
            if (row.dad_data != None):
                row.dad_data = row.dad_data.isoformat()
            else:
                row.dad_data = ""
            inserir_dados.apply_async((row.dad_id, row.est_id, row.cam_id, row.dad_data, row.dad_1med, row.dad_1stat,
                                row.dad_2med, row.dad_2stat, row.dad_3med, row.dad_3stat, row.dad_4med, row.dad_4stat,
                                row.dad_5med, row.dad_5stat, row.dad_6med, row.dad_6stat, row.dad_7med, row.dad_7stat,
                                row.dad_8med, row.dad_8stat, row.dad_9med, row.dad_9stat,
                                row.dad_10med, row.dad_10stat, row.dad_11med, row.dad_11stat, row.dad_12med,
                                row.dad_12stat, row.dad_13med, row.dad_13stat, row.dad_14med, row.dad_14stat,
                                row.dad_15med, row.dad_15stat, row.dad_16med, row.dad_16stat, row.dad_17med,
                                row.dad_17stat, row.dad_18med, row.dad_18stat, row.dad_19med, row.dad_19stat,
                                row.dad_20med, row.dad_20stat, row.dad_p_aquisicao), retry=True, retry_policy={
                    'max_retries': 0,  # retry until sent
                    'interval_start': 60,  # start retrying one minute later
                    'interval_step': 30,  # wait 30 seconds before retrying again
                    'interval_max': 180,
                })
            last_entry_id = row.dad_id - 1

    cursor.execute('UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'Dados\';')
    cursor.commit()


# INSERIR ESTADO
def adicionar_estado(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'Estado\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute(
        "select est_id, est_data, est_1med, est_1mode, est_1valido, est_1alarme, est_2med, est_2mode, est_2valido, est_2alarme, est_3med, est_3mode, est_3valido, est_3alarme, "
        "est_4med, est_4mode, est_4valido, est_4alarme, est_5med, est_5mode, est_5valido, est_5alarme, est_6med, est_6mode, est_6valido, est_6alarme, "
        "est_7med, est_7mode, est_7valido, est_7alarme, est_8med, est_8mode, est_8valido, est_8alarme, est_9med, est_9mode, est_9valido, est_9alarme, "
        "est_10med, est_10mode, est_10valido, est_10alarme, est_11med, est_11mode, est_11valido, est_11alarme, est_12med, est_12mode, est_12valido, est_12alarme, "
        "est_13med, est_13mode, est_13valido, est_13alarme, est_14med, est_14mode, est_14valido, est_14alarme, est_15med, est_15mode, est_15valido, est_15alarme, "
        "est_16med, est_16mode, est_16valido, est_16alarme, est_17med, est_17mode, est_17valido, est_17alarme, est_18med, est_18mode, est_18valido, est_18alarme, "
        "est_19med, est_19mode, est_19valido, est_19alarme, est_20med, est_20mode, est_20valido, est_20alarme from estado")
    for row in cursor.fetchall():
        if (row.est_id > last_entry_id):
            if (row.est_data != None):
                row.est_data = row.est_data.isoformat()
            else:
                row.est_data = ""
            inserir_estado.apply_async((row.est_id, row.est_data, row.est_1med, row.est_1mode, row.est_1valido,
                                 row.est_1alarme, row.est_2med, row.est_2mode, row.est_2valido, row.est_2alarme,
                                 row.est_3med, row.est_3mode, row.est_3valido, row.est_3alarme, row.est_4med,
                                 row.est_4mode, row.est_4valido, row.est_4alarme,
                                 row.est_5med, row.est_5mode, row.est_5valido, row.est_5alarme, row.est_6med,
                                 row.est_6mode, row.est_6valido, row.est_6alarme,
                                 row.est_7med, row.est_7mode, row.est_7valido, row.est_7alarme, row.est_8med,
                                 row.est_8mode, row.est_8valido, row.est_8alarme,
                                 row.est_9med, row.est_9mode, row.est_9valido, row.est_9alarme, row.est_10med,
                                 row.est_10mode, row.est_10valido, row.est_10alarme,
                                 row.est_11med, row.est_11mode, row.est_11valido, row.est_11alarme, row.est_12med,
                                 row.est_12mode, row.est_12valido, row.est_12alarme,
                                 row.est_13med, row.est_13mode, row.est_13valido, row.est_13alarme, row.est_14med,
                                 row.est_14mode, row.est_14valido, row.est_14alarme,
                                 row.est_15med, row.est_15mode, row.est_15valido, row.est_15alarme, row.est_16med,
                                 row.est_16mode, row.est_16valido, row.est_16alarme,
                                 row.est_17med, row.est_17mode, row.est_17valido, row.est_17alarme, row.est_18med,
                                 row.est_18mode, row.est_18valido, row.est_18alarme,
                                 row.est_19med, row.est_19mode, row.est_19valido, row.est_19alarme, row.est_20med,
                                 row.est_20mode, row.est_20valido, row.est_20alarme), retry=True, retry_policy={
                    'max_retries': 0,  # retry until sent
                    'interval_start': 60,  # start retrying one minute later
                    'interval_step': 30,  # wait 30 seconds before retrying again
                    'interval_max': 180,
                })
            last_entry_id = row.est_id

    cursor.execute('UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'Estado\';')
    cursor.commit()


# INSERIR RECOLHA
def adicionar_recolha(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'Recolha\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute("select rec_id, est_id, rec_hora, rec_dias_semana from recolha")
    for row in cursor.fetchall():
        if(row.rec_id > last_entry_id):
        	inserir_recolha.apply_async((row.rec_id, row.est_id, row.rec_hora, row.rec_dias_semana), retry=True, retry_policy={
	            'max_retries': 0,  # retry until sent
	            'interval_start': 60,  # start retrying one minute later
	            'interval_step': 30,  # wait 30 seconds before retrying again
	            'interval_max': 180,
	        })
        	last_entry_id = row.rec_id

    cursor.execute('UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'Recolha\';')
    cursor.commit()


# INSERIR UNIDADE
def adicionar_unidade(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'Unidade\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute(
        "select uni_id, est_id, uni_tipo, uni_num_serie, uni_nome, uni_ual_lres, uni_ual_hres, uni_dt_programa, uni_ual_prodid, uni_ual_majver, uni_ual_minver, "
        "uni_ual_revver, uni_sam_calib_zero, uni_sam_calib_max, uni_sam_sinc_calibh, uni_sam_sinc_calibm, uni_sam_ciclo_calib, uni_asm_endereco, uni_asm_offset, "
        "uni_asm_gama, uni_asm_tempo_resp, uni_asm_unidade, uni_asm_calib_zero_ref, uni_asm_calib_zero, uni_asm_calib_auto, uni_asm_calib_max, uni_asm_dur_zero, "
        "uni_asm_dur_max, uni_asm_span, uni_asm_banco, uni_versao, uni_modelo, uni_suspenso, uni_dt_recolha, uni_tea_tavrg, uni_tea_ctemp, uni_tea_cpres, uni_asm_modo, "
        "uni_ccr_nloca_amostra, uni_ccr_nloca_total, uni_dtl_password, uni_obs_intervalo_s, uni_obs_intervalo_p, uni_obs_intervalo_q, uni_obs_bateria, uni_obs_cartridge, "
        "uni_obs_apaga_dados, uni_ccr_nloca_eventos, uni_ccr_nloca_total_eventos from unidade")
    for row in cursor.fetchall():
        if (row.uni_id > last_entry_id):
            if (row.uni_dt_programa != None):
                row.uni_dt_programa = row.uni_dt_programa.isoformat()
            else:
                row.uni_dt_programa = ""
            if (row.uni_dt_recolha != None):
                row.uni_dt_recolha = row.uni_dt_recolha.isoformat()
            else:
                row.uni_dt_recolha = ""
            inserir_unidade.apply_async((row.uni_id, row.est_id, row.uni_tipo, row.uni_num_serie, row.uni_nome,
                                  row.uni_ual_lres, row.uni_ual_hres, row.uni_dt_programa, row.uni_ual_prodid,
                                  row.uni_ual_majver, row.uni_ual_minver, row.uni_ual_revver, row.uni_sam_calib_zero,
                                  row.uni_sam_calib_max, row.uni_sam_sinc_calibh, row.uni_sam_sinc_calibm,
                                  row.uni_sam_ciclo_calib, row.uni_asm_endereco, row.uni_asm_offset, row.uni_asm_gama,
                                  row.uni_asm_tempo_resp, row.uni_asm_unidade, row.uni_asm_calib_zero_ref,
                                  row.uni_asm_calib_zero, row.uni_asm_calib_auto, row.uni_asm_calib_max,
                                  row.uni_asm_dur_zero, row.uni_asm_dur_max, row.uni_asm_span, row.uni_asm_banco,
                                  row.uni_versao, row.uni_modelo, row.uni_suspenso, row.uni_dt_recolha,
                                  row.uni_tea_tavrg, row.uni_tea_ctemp, row.uni_tea_cpres, row.uni_asm_modo,
                                  row.uni_ccr_nloca_amostra,
                                  row.uni_ccr_nloca_total, row.uni_dtl_password, row.uni_obs_intervalo_s,
                                  row.uni_obs_intervalo_p, row.uni_obs_intervalo_q, row.uni_obs_bateria,
                                  row.uni_obs_cartridge, row.uni_obs_apaga_dados, row.uni_ccr_nloca_eventos,
                                  row.uni_ccr_nloca_total_eventos), retry=True, retry_policy={
                    'max_retries': 0,  # retry until sent
                    'interval_start': 60,  # start retrying one minute later
                    'interval_step': 30,  # wait 30 seconds before retrying again
                    'interval_max': 180,
                })
            last_entry_id = row.uni_id

    cursor.execute('UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'Unidade\';')
    cursor.commit()


# INSERIR ESTADO_INTERNO
def adicionar_estado_interno(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'EstadoInterno\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute(
        "select uni_id, est_data, est_1nome, est_1unidmed, est_1valor, est_1modo, est_1valido, est_2nome, est_2unidmed, est_2valor, est_2modo, est_2valido, "
        "est_3nome, est_3unidmed, est_3valor, est_3modo, est_3valido, est_4nome, est_4unidmed, est_4valor, est_4modo, est_4valido, "
        "est_5nome, est_5unidmed, est_5valor, est_5modo, est_5valido, est_6nome, est_6unidmed, est_6valor, est_6modo, est_6valido, "
        "est_7nome, est_7unidmed, est_7valor, est_7modo, est_7valido, est_8nome, est_8unidmed, est_8valor, est_8modo, est_8valido, "
        "est_9nome, est_9unidmed, est_9valor, est_9modo, est_9valido, est_10nome, est_10unidmed, est_10valor, est_10modo, est_10valido, "
        "est_11nome, est_11unidmed, est_11valor, est_11modo, est_11valido, est_12nome, est_12unidmed, est_12valor, est_12modo, est_12valido, "
        "est_13nome, est_13unidmed, est_13valor, est_13modo, est_13valido, est_14nome, est_14unidmed, est_14valor, est_14modo, est_14valido, "
        "est_15nome, est_15unidmed, est_15valor, est_15modo, est_15valido, est_16nome, est_16unidmed, est_16valor, est_16modo, est_16valido from estado_interno")
    for row in cursor.fetchall():
        if (row.uni_id > last_entry_id):
            if (row.est_data != None):
                row.est_data = row.est_data.isoformat()
            else:
                row.est_data = ""
            inserir_estado_interno.apply_async((row.uni_id, row.est_data, row.est_1nome, row.est_1unidmed, row.est_1valor,
                                         row.est_1modo, row.est_1valido,
                                         row.est_2nome, row.est_2unidmed, row.est_2valor, row.est_2modo,
                                         row.est_2valido, row.est_3nome, row.est_3unidmed, row.est_3valor,
                                         row.est_3modo, row.est_3valido,
                                         row.est_4nome, row.est_4unidmed, row.est_4valor, row.est_4modo,
                                         row.est_4valido, row.est_5nome, row.est_5unidmed, row.est_5valor,
                                         row.est_5modo, row.est_5valido,
                                         row.est_6nome, row.est_6unidmed, row.est_6valor, row.est_6modo,
                                         row.est_6valido, row.est_7nome, row.est_7unidmed, row.est_7valor,
                                         row.est_7modo, row.est_7valido,
                                         row.est_8nome, row.est_8unidmed, row.est_8valor, row.est_8modo,
                                         row.est_8valido, row.est_9nome, row.est_9unidmed, row.est_9valor,
                                         row.est_9modo, row.est_9valido,
                                         row.est_10nome, row.est_10unidmed, row.est_10valor, row.est_10modo,
                                         row.est_10valido, row.est_11nome, row.est_11unidmed, row.est_11valor,
                                         row.est_11modo,
                                         row.est_11valido, row.est_12nome, row.est_12unidmed, row.est_12valor,
                                         row.est_12modo, row.est_12valido, row.est_13nome, row.est_13unidmed,
                                         row.est_13valor,
                                         row.est_13modo, row.est_13valido, row.est_14nome, row.est_14unidmed,
                                         row.est_14valor, row.est_14modo, row.est_14valido, row.est_15nome,
                                         row.est_15unidmed,
                                         row.est_15valor, row.est_15modo, row.est_15valido, row.est_16nome,
                                         row.est_16unidmed, row.est_16valor, row.est_16modo, row.est_16valido),
                                         retry=True, retry_policy={
                    'max_retries': 0,  # retry until sent
                    'interval_start': 60,  # start retrying one minute later
                    'interval_step': 30,  # wait 30 seconds before retrying again
                    'interval_max': 180,
                })
            last_entry_id = row.uni_id

    cursor.execute(
        'UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'EstadoInterno\';')
    cursor.commit()


# INSERIR PARAMETRO
def adicionar_parametro(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'Parametro\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute(
        "select par_id, cat_id, par_nome, par_nome_completo, par_codigo, par_unidade_armaz, par_unidade_apresent, par_conv_mul, par_conv_add, par_casas_decimais, par_limiar, "
        "par_limite, par_perc_validacao_hora, par_perc_validacao_dia, par_media_tipo, par_media_intervalo, par_visivel, par_min_alarme, par_max_alarme, par_limite_media, "
        "par_tipo from parametro")
    for row in cursor.fetchall():
        if (row.par_id > last_entry_id):
            inserir_parametro.apply_async((row.par_id, row.cat_id, row.par_nome, row.par_nome_completo, row.par_codigo,
                                    row.par_unidade_armaz, row.par_unidade_apresent, row.par_conv_mul,
                                    row.par_conv_add, row.par_casas_decimais, row.par_limiar, row.par_limite,
                                    row.par_perc_validacao_hora, row.par_perc_validacao_dia, row.par_media_tipo,
                                    row.par_media_intervalo, row.par_visivel, row.par_min_alarme, row.par_max_alarme,
                                    row.par_limite_media, row.par_tipo), retry=True, retry_policy={
                    'max_retries': 0,  # retry until sent
                    'interval_start': 60,  # start retrying one minute later
                    'interval_step': 30,  # wait 30 seconds before retrying again
                    'interval_max': 180,
                })
            last_entry_id = row.par_id

    cursor.execute('UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'Parametro\';')
    cursor.commit()


# INSERIR CANAL
def adicionar_canal(cursor):
    last_entry = cursor.execute("select ultimo_id from connector_helper where tabela_nome=\'Canal\'").fetchone()
    last_entry_id = last_entry.ultimo_id

    cursor.execute(
        "select can_id, uni_id, par_id, can_num_estacao, can_num_unidade, can_unidade_medida, can_conv_med_mul, can_conv_med_add, can_min_medida, can_max_medida, "
        "can_min_alarme, can_max_alarme, can_validacao, can_suspenso, can_background, can_coeficiente, can_num_monit, ual_meteo, ual_sin_cmd, ual_slope, ual_y0, "
        "ual_nome, sam_ganho, sam_sin_alarme, sam_sin_zero, sam_sin_max, sam_cmd_zero, sam_cmd_max, vir_formula, uar_tipo, uar_calculo, uar_num_entrada, can_reg_eventos, can_addr_eventos, "
        "can_acumula_eventos from canal")
    for row in cursor.fetchall():
        if (row.can_id > last_entry_id):
            inserir_canal.apply_async((row.can_id, row.uni_id, row.par_id, row.can_num_estacao, row.can_num_unidade,
                                row.can_unidade_medida, row.can_conv_med_mul, row.can_conv_med_add,
                                row.can_min_medida, row.can_max_medida, row.can_min_alarme, row.can_max_alarme,
                                row.can_validacao, row.can_suspenso, row.can_background, row.can_coeficiente,
                                row.can_num_monit, row.ual_meteo, row.ual_sin_cmd, row.ual_slope, row.ual_y0,
                                row.ual_nome, row.sam_ganho, row.sam_sin_alarme, row.sam_sin_zero, row.sam_sin_max,
                                row.sam_cmd_zero, row.sam_cmd_max, row.vir_formula, row.uar_tipo, row.uar_calculo,
                                row.uar_num_entrada, row.can_reg_eventos, row.can_addr_eventos,
                                row.can_acumula_eventos), retry=True, retry_policy={
                    'max_retries': 0,  # retry until sent
                    'interval_start': 60,  # start retrying one minute later
                    'interval_step': 30,  # wait 30 seconds before retrying again
                    'interval_max': 180,
                })
            last_entry_id = row.can_id

    cursor.execute('UPDATE connector_helper SET ultimo_id=' + str(last_entry_id) + ' WHERE tabela_nome=\'Canal\';')
    cursor.commit()


# MAIN CODE

cnxn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=EMMQAR-PC\SQLEXPRESS;DATABASE=Atmis; trusted_connection=true", autocommit=True')
cursor = cnxn.cursor()
try:
    cursor.execute("select * from Atmis.dbo.connector_helper;")
except Exception, e:
    cursor.execute(
        "create table Atmis.dbo.connector_helper(tabela_nome varchar(50) not null, ultimo_id int not null, primary key(tabela_nome));")
    cursor.execute('insert into connector_helper values(\'Rede\', -1)')
    cursor.execute('insert into connector_helper values(\'Mapa\', -1)')
    cursor.execute('insert into connector_helper values(\'Estacao\', -1)')
    cursor.execute('insert into connector_helper values(\'Dados\', -1)')
    cursor.execute('insert into connector_helper values(\'Estado\', -1)')
    cursor.execute('insert into connector_helper values(\'Recolha\', -1)')
    cursor.execute('insert into connector_helper values(\'Unidade\', -1)')
    cursor.execute('insert into connector_helper values(\'EstadoInterno\', -1)')
    cursor.execute('insert into connector_helper values(\'Parametro\', -1)')
    cursor.execute('insert into connector_helper values(\'Canal\', -1)')
    cursor.commit()


adicionar_rede(cursor)
adicionar_mapa(cursor)
adicionar_estacao(cursor)
adicionar_dados(cursor)
adicionar_estado(cursor)
adicionar_recolha(cursor)
adicionar_unidade(cursor)
adicionar_estado_interno(cursor)
adicionar_parametro(cursor)
adicionar_canal(cursor)
