const enviarEmail = require('../../handlers/email');
const client = require('../config/db.client');
const moment=require('moment');
const QRCode = require("qrcode");
const PDF = require('pdfkit');//Importando la libreria de PDFkit
const fs = require('fs');
const { verify } = require('crypto');
const path = require('path');
const Notifications= require('../../handlers/notifications');
const emailHandler = require('../../apis/emailHandler');
const funcionesCompartidasCtrl = require('./funcionesCompartidasCtrl.js');


exports.orquestador_server_2 = async (req, resp) => {
    console.log("$.$");
    console.log("$.$");
    console.log("SCHEDULER MAXIMISE");
    var sche_envio_maximise = require('node-schedule');

    sche_envio_maximise.scheduleJob('*/60 * * * * *', () => {
        console.log('');
        console.log("CONSULTANDO COLA ");
        console.log("$$$");
        orquestador_server_2();

    });

    async function orquestador_server_2() {
        var tarea = await client.query(` SELECT * FROM public.queue_maximise WHERE estado='PENDIENTE' ORDER BY id ASC limit 1`);
        console.log("$$$$$$");
        if(tarea.rows.length > 0) {
            console.log('TAREA ENCONTRADA: ENVIO DE ' + tarea.rows[0]['tarea']);

            if (tarea.rows[0]['tarea'] == 'PAGOS') {
                enviar_pago_maximise(tarea.rows[0])

            } else if (tarea.rows[0]['tarea'] == 'PAGOS USD') {
                enviar_pago_usd_maximise(tarea.rows[0])
            }

        } else {
            console.log('NO HAY TAREAS PENDIENTES EN COLA');
        }
    }

    async function enviar_pago_maximise(tarea) {
        console.log('Envio de pago')
        client.query(` UPDATE public.queue_maximise SET estado='PROCESANDO' WHERE id=${tarea.id} `);

        var carpeta_data = await client.query(` SELECT id, fk_responsable, errores, estado, cmpy_code, year_num, period_num, jour_code, entry_code, debit_amt, credit_amt, post_flag, com_text, cleared_flag, control_amt, debit_amt1, credit_amt1, debit_amt2, credit_amt2, debit_amt3, credit_amt3, tran_type_ind, ledger_code, fk_despacho, fk_nota_cobro, carpeta, codigo_cliente, monto, descripcion_movimiento, saldo, fecha_ingreso, fecha_registro, "fk_createdBy", "fk_updatedBy", "createdAt", "updatedAt", codigo_bi_pago, conversion, razon_social,
                                                    to_char(entry_date, 'DD-MM-YYYY') as entry_date,
                                                    to_char(jour_date, 'DD-MM-YYYY') as jour_date,
                                                    to_char(fecha_movimiento, 'DD-MM-YYYY') as fecha_movimiento
                                                    FROM public.wsc_envio_asientos_cabeceras
                                                where id=${tarea['fk_cabecera']}`);

        if(carpeta_data.rows.length>0) {

            let cmpy_code = carpeta_data.rows[0].cmpy_code;
            let year_num = carpeta_data.rows[0].year_num;
            let period_num = carpeta_data.rows[0].period_num;
            let jour_code = carpeta_data.rows[0].jour_code;
            let entry_code = carpeta_data.rows[0].entry_code;
            let entry_date = carpeta_data.rows[0].entry_date;
            let jour_date = carpeta_data.rows[0].jour_date;
            let debit_amt = carpeta_data.rows[0].debit_amt;
            let credit_amt = carpeta_data.rows[0].credit_amt;
            let post_flag = carpeta_data.rows[0].post_flag;
            let com_text = carpeta_data.rows[0].com_text;
            let cleared_flag = carpeta_data.rows[0].cleared_flag;
            let control_amt = carpeta_data.rows[0].control_amt;
            let debit_amt1 = carpeta_data.rows[0].debit_amt1;
            let credit_amt1 = carpeta_data.rows[0].credit_amt1;
            let debit_amt2 = carpeta_data.rows[0].debit_amt2;
            let credit_amt2 = carpeta_data.rows[0].credit_amt2;
            let tran_type_ind = carpeta_data.rows[0].tran_type_ind;
            let ledger_code = carpeta_data.rows[0].ledger_code;
            let debit_amt3 = carpeta_data.rows[0].debit_amt3;
            let credit_amt3 = carpeta_data.rows[0].credit_amt3;
            let razon_social = carpeta_data.rows[0].razon_social;

            let carpeta = carpeta_data.rows[0].carpeta;
            let monto = carpeta_data.rows[0].monto;
            let codigo_cliente = carpeta_data.rows[0].codigo_cliente;
            let fecha_movimiento = carpeta_data.rows[0].fecha_movimiento;

            var xmltext_aux = `<string xmlns="Maximise"><BATCH><HEAD>`;
            xmltext_aux += `<CMPY_CODE>`+ cmpy_code +`</CMPY_CODE>`;
            xmltext_aux += `<YEAR_NUM>`+ year_num +`</YEAR_NUM>`;
            xmltext_aux += `<PERIOD_NUM>`+ period_num +`</PERIOD_NUM>`;
            xmltext_aux += `<JOUR_CODE>`+ jour_code +`</JOUR_CODE>`;
            xmltext_aux += `<ENTRY_CODE>`+ entry_code +`</ENTRY_CODE>`;
            xmltext_aux += `<ENTRY_DATE>`+ entry_date +`</ENTRY_DATE>`;
            xmltext_aux += `<JOUR_DATE>`+ jour_date +`</JOUR_DATE>`;
            xmltext_aux += `<DEBIT_AMT>`+ debit_amt +`</DEBIT_AMT>`;
            xmltext_aux += `<CREDIT_AMT>`+ credit_amt +`</CREDIT_AMT>`;
            xmltext_aux += `<POST_FLAG>`+ post_flag +`</POST_FLAG>`;
            xmltext_aux += `<COM_TEXT>`+ com_text + tarea['fk_cabecera'] +`</COM_TEXT>`;
            xmltext_aux += `<CLEARED_FLAG>`+ cleared_flag +`</CLEARED_FLAG>`;
            xmltext_aux += `<CONTROL_AMT>`+ control_amt +`</CONTROL_AMT>`;
            xmltext_aux += `<DEBIT_AMT1>`+ debit_amt1 +`</DEBIT_AMT1>`;
            xmltext_aux += `<CREDIT_AMT1>`+ credit_amt1 +`</CREDIT_AMT1>`;
            xmltext_aux += `<DEBIT_AMT2>`+ debit_amt2 +`</DEBIT_AMT2>`;
            xmltext_aux += `<CREDIT_AMT2>`+ credit_amt2 +`</CREDIT_AMT2>`;
            xmltext_aux += `<TRAN_TYPE_IND>`+ tran_type_ind +`</TRAN_TYPE_IND>`;
            xmltext_aux += `<LEDGER_CODE>`+ ledger_code +`</LEDGER_CODE>`;
            xmltext_aux += `<DEBIT_AMT3>`+ debit_amt3 +`</DEBIT_AMT3>`;
            xmltext_aux += `<CREDIT_AMT3>`+ credit_amt3 +`</CREDIT_AMT3>`;
            xmltext_aux += `<CURRENCY_IND >`+ '1' +`</CURRENCY_IND >`;
            xmltext_aux += `</HEAD>`;

            let pago = {
                n_carpeta: carpeta,
                monto: monto,
                cliente: codigo_cliente,
                fecha: fecha_movimiento
            }

            consultar_carpeta_maximise(tarea['fk_cabecera'], pago, xmltext_aux, razon_social);

        }

        async function consultar_carpeta_maximise(fk_cabecera, pago, xmltext_aux, razon_social) {

            const request = require('request');
            const fs = require("fs");

            var xml_cws = xmltext_aux.split('CMPY_CODE')[0] + 'CMPY_CODE>03</CMPY_CODE' + xmltext_aux.split('CMPY_CODE')[2];

            const xmltext = `SELECT ih.INV_NUM, ih.doc_code, ih.cust_code, ih.PAID_AMT, ih.TOTAL_AMT,ih.REF_TEXT1,ih.REF_TEXT2, ih.ext_num,
                            (SELECT top 1 TOTAL_AMT FROM credithead ch WHERE ch.inv_num = ih.inv_num AND POSTED_FLAG<>'V' AND (CMPY_CODE='02' OR CMPY_CODE='03' OR CMPY_CODE='04')) as total_credito,
                            (SELECT SUM(TOTAL_AMT) - SUM(PAID_AMT) FROM invoicehead WHERE cmpy_code='03' AND ref_text1='${pago.n_carpeta}' AND ref_text2='PALLET' AND POSTED_FLAG<>'V') as pallet,
                            (SELECT SUM(TOTAL_AMT) - SUM(PAID_AMT) FROM invoicehead WHERE cmpy_code='03' AND ref_text1='${pago.n_carpeta}' AND ref_text2='TVP' AND POSTED_FLAG<>'V') as tvp,
                            (SELECT SUM(TOTAL_AMT) - SUM(PAID_AMT) FROM invoicehead WHERE cmpy_code='03' AND ref_text1='${pago.n_carpeta}' AND ref_text2='OTROS' AND POSTED_FLAG<>'V') as otros
                            FROM invoicehead ih
                            WHERE ih.cmpy_code='04' and
                            ih.POSTED_FLAG<>'V' and
                            ih.ref_text1='${pago.n_carpeta}'
                            ORDER BY ih.doc_code asc;`;

            const data = {
                AliasName: 'dwi_tnm',
                UserName: 'webservice',
                Password: 'webservice001',
                Sql: xmltext
            };

            const options = {
                url: 'http://asp3.maximise.cl/wsv/query.asmx/GetQueryAsDataSet',
                method: 'POST',
                headers: {
                    'Content-Type': 'text/html',
                    'Content-Length': data.length
                },
                json: true,
                form: {
                    AliasName: 'dwi_tnm',
                    UserName: 'webservice',
                    Password: 'webservice001',
                    Sql: xmltext
                }
            };

            try {

                request.post(options, async(err, res, body) => {

                    if(err)
                    {
                        console.log('ERROR ' + pago.n_carpeta + ', ' + err);
                    }
                    else if(body)
                    {
                        if(res.statusCode!=200){

                            console.log('ERROR ' + pago.n_carpeta + ', ' + JSON.stringify(res.body));

                        }
                        else if(res.statusCode==200 )
                        {
                            var documentos = [];
                            var payment = Number(pago.monto);

                            var entries = JSON.stringify(body).split('<Table');

                            let resp_cust_code = '';
                            let nota_debito = 0;

                            //GUARDO DOCUMENTOS EN ARRAY DE OBJETOS
                            for(var j=1; j<entries.length; j++) {
                                let resp_inv_num = entries[j].split('<INV_NUM>').pop().split('</INV_NUM>')[0];
                                let resp_doc_code = entries[j].split('<doc_code>').pop().split('</doc_code>')[0];
                                resp_cust_code = entries[j].split('<cust_code>').pop().split('</cust_code>')[0];
                                let resp_paid_amt = entries[j].split('<PAID_AMT>').pop().split('</PAID_AMT>')[0];
                                let resp_total_amt = entries[j].split('<TOTAL_AMT>').pop().split('</TOTAL_AMT>')[0];
                                let resp_ref_text1 = entries[j].split('<REF_TEXT1>').pop().split('</REF_TEXT1>')[0];
                                let resp_ref_text2 = entries[j].split('<REF_TEXT2>').pop().split('</REF_TEXT2>')[0];
                                let resp_ext_num = entries[j].split('<ext_num>').pop().split('</ext_num>')[0];
                                let resp_total_credito = entries[j].split('<total_credito>').pop().split('</total_credito>')[0];
                                let resp_cred_num = entries[j].split('<cred_num>').pop().split('</cred_num>')[0];
                                let resp_pallet = entries[j].split('<pallet>').pop().split('</pallet>')[0];
                                let resp_tvp = entries[j].split('<tvp>').pop().split('</tvp>')[0];
                                let resp_otros = entries[j].split('<otros>').pop().split('</otros>')[0];

                                let resp_por_pagar = Number(resp_total_amt) - Number(resp_paid_amt);

                                try {
                                    if (Number(resp_total_credito) > 0) {
                                        resp_por_pagar -= Number(resp_total_credito)
                                    }
                                } catch {

                                }

                                if (resp_doc_code == 'F8') {
                                    resp_por_pagar += nota_debito;
                                }

                                if (resp_doc_code == 'D1'){
                                    nota_debito = Number(resp_total_amt)

                                //} else if (resp_doc_code != 'P8' && resp_por_pagar > 10) {
                                } else if (resp_por_pagar > 10) {
                                    documentos.push({
                                        resp_inv_num: resp_inv_num,
                                        resp_doc_code: resp_doc_code,
                                        resp_paid_amt: resp_paid_amt,
                                        resp_total_amt: resp_total_amt,
                                        resp_por_pagar: resp_por_pagar,
                                        resp_ref_text1: resp_ref_text1,
                                        resp_ref_text2: resp_ref_text2,
                                        resp_ext_num: resp_ext_num
                                    })
                                }

                                if (j+1 == entries.length) {
                                    if (! isNaN(resp_pallet) ) {
                                        documentos.push({
                                            resp_inv_num: resp_inv_num,
                                            resp_doc_code: 'PALLET',
                                            resp_por_pagar: Number(resp_pallet),
                                        })
                                    }
                                    if (! isNaN(resp_tvp) ) {
                                        documentos.push({
                                            resp_inv_num: resp_inv_num,
                                            resp_doc_code: 'TVP',
                                            resp_por_pagar: Number(resp_tvp),
                                        })
                                    }
                                    if (! isNaN(resp_otros) ) {
                                        documentos.push({
                                            resp_inv_num: resp_inv_num,
                                            resp_doc_code: 'OTROS',
                                            resp_por_pagar: Number(resp_otros),
                                        })
                                    }
                                }

                            }

                            if (documentos.length == 0) {
                                //DINERO SE IRA A EXCEDENTE, PERO HAY CAMPOS QUE DEBO CONSULTAR DESDE OTRA FUENTE
                                resp_cust_code = await get_cliente_rut(pago.cliente.split(' ')[0]);
                                if (resp_cust_code == false) {
                                    return false;
                                }
                            }

                            // BANCO BANCO BANCO BANCO BANCO BANCO BANCO BANCO BANCO

                            var auto_num = 1;

                            let bco_fk_cabecera = fk_cabecera;
                            let bco_seq_num = auto_num;
                            let bco_analysis_text = '';
                            let bco_tran_date = pago.fecha;
                            let bco_ref_text = resp_cust_code;
                            let bco_ref_num = 0;
                            let bco_debit_amt = payment;
                            let bco_credit_amt = 0;
                            let bco_debit_amt1 = 0;
                            let bco_credit_amt1 = 0;
                            let bco_debit_amt2 = 0;
                            let bco_credit_amt2 = 0;
                            let bco_debit_amt3 = 0;
                            let bco_credit_amt3 = 0;
                            let bco_ref_text1 = pago.n_carpeta;
                            let bco_bran_code = '';
                            let bco_profit_code = '';
                            let bco_currency_code = 'CLP';
                            let bco_rate_exchange = 1;

                            let bco_acct_code = '11103001-000';
                            let bco_ref_text2 = resp_cust_code;
                            let bco_desc_text = 'BANCO SANTANDER';

                            columna = '';                           valor = '';
                            columna+=`"fk_cabecera",`;              valor+=`'`+ bco_fk_cabecera +`',`;
                            columna+=`"seq_num",`;                  valor+=`'`+ bco_seq_num +`',`;
                            columna+=`"analysis_text",`;            valor+=`'`+ bco_analysis_text +`',`;
                            columna+=`"tran_date",`;                valor+=`'`+ bco_tran_date +`',`;
                            columna+=`"ref_text",`;                 valor+=`'`+ bco_ref_text +`',`;
                            columna+=`"ref_num",`;                  valor+=`'`+ bco_ref_num +`',`;
                            columna+=`"debit_amt",`;                valor+=`'`+ bco_debit_amt +`',`;
                            columna+=`"credit_amt",`;               valor+=`'`+ bco_credit_amt +`',`;
                            columna+=`"debit_amt1",`;               valor+=`'`+ bco_debit_amt1 +`',`;
                            columna+=`"credit_amt1",`;              valor+=`'`+ bco_credit_amt1 +`',`;
                            columna+=`"debit_amt2",`;               valor+=`'`+ bco_debit_amt2 +`',`;
                            columna+=`"credit_amt2",`;              valor+=`'`+ bco_credit_amt2 +`',`;
                            columna+=`"debit_amt3",`;               valor+=`'`+ bco_debit_amt3 +`',`;
                            columna+=`"credit_amt3",`;              valor+=`'`+ bco_credit_amt3 +`',`;
                            columna+=`"ref_text1",`;                valor+=`'`+ bco_ref_text1 +`',`;
                            columna+=`"bran_code",`;                valor+=`'`+ bco_bran_code +`',`;
                            columna+=`"profit_code",`;              valor+=`'`+ bco_profit_code +`',`;
                            columna+=`"currency_code",`;            valor+=`'`+ bco_currency_code +`',`;
                            columna+=`"rate_exchange",`;            valor+=`'`+ bco_rate_exchange +`',`;
                            columna+=`"acct_code",`;                valor+=`'`+ bco_acct_code +`',`;
                            columna+=`"ref_text2",`;                valor+=`'`+ bco_ref_text2 +`',`;
                            columna+=`"desc_text"`;                 valor+=`'`+ bco_desc_text +`'`;

                            save_detalle(columna, valor);
                            /** XML DETALLE BANCO  **/
                            xmltext_aux += `<DETAIL>`;
                            xmltext_aux += `<CMPY_CODE>`+ '04' +`</CMPY_CODE>`;
                            xmltext_aux += `<SEQ_NUM>`+ bco_seq_num +`</SEQ_NUM>`;
                            xmltext_aux += `<TRAN_TYPE_IND>`+ 'AJU' +`</TRAN_TYPE_IND>`;
                            xmltext_aux += `<ANALYSIS_TEXT>`+ bco_analysis_text +`</ANALYSIS_TEXT>`;
                            xmltext_aux += `<TRAN_DATE>`+ bco_tran_date +`</TRAN_DATE>`;
                            xmltext_aux += `<REF_TEXT>`+ bco_ref_text +`</REF_TEXT>`;
                            xmltext_aux += `<REF_NUM>`+ bco_ref_num +`</REF_NUM>`;
                            xmltext_aux += `<ACCT_CODE>`+ bco_acct_code +`</ACCT_CODE>`;
                            xmltext_aux += `<DEBIT_AMT>`+ bco_debit_amt +`</DEBIT_AMT>`;
                            xmltext_aux += `<CREDIT_AMT>`+ bco_credit_amt +`</CREDIT_AMT>`;
                            xmltext_aux += `<DEBIT_AMT1>`+ bco_debit_amt1 +`</DEBIT_AMT1>`;
                            xmltext_aux += `<CREDIT_AMT1>`+ bco_credit_amt1 +`</CREDIT_AMT1>`;
                            xmltext_aux += `<DEBIT_AMT2>`+ bco_debit_amt2 +`</DEBIT_AMT2>`;
                            xmltext_aux += `<CREDIT_AMT2>`+ bco_credit_amt2 +`</CREDIT_AMT2>`;
                            xmltext_aux += `<REF_TEXT1>`+ bco_ref_text1 +`</REF_TEXT1>`;
                            xmltext_aux += `<REF_TEXT2>`+ bco_ref_text2 +`</REF_TEXT2>`;
                            xmltext_aux += `<DESC_TEXT>`+ bco_desc_text.replace('&', 'y').replace('&', 'y') +`</DESC_TEXT>`;
                            xmltext_aux += `<BRAN_CODE>`+ bco_bran_code +`</BRAN_CODE>`;
                            xmltext_aux += `<PROFIT_CODE>`+ bco_profit_code +`</PROFIT_CODE>`;
                            xmltext_aux += `<CURRENCY_CODE>`+ bco_currency_code +`</CURRENCY_CODE>`;
                            xmltext_aux += `<RATE_EXCHANGE>`+ bco_rate_exchange +`</RATE_EXCHANGE>`;
                            xmltext_aux += `<DEBIT_AMT3>`+ bco_debit_amt3 +`</DEBIT_AMT3>`;
                            xmltext_aux += `<CREDIT_AMT3>`+ bco_credit_amt3 +`</CREDIT_AMT3>`;

                            xmltext_aux += `<CURRENCY_IND >`+ '1' +`</CURRENCY_IND >`;

                            xmltext_aux += `</DETAIL>`;

                            auto_num++;
                            // DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO
                            for (let i=0; i<documentos.length; i++) {

                                var curr_pay = 0;
                                if(payment - documentos[i].resp_por_pagar >= 0) {
                                    curr_pay = documentos[i].resp_por_pagar;
                                    payment -= documentos[i].resp_por_pagar;
                                } else {
                                    curr_pay = payment;
                                    payment = 0;
                                }

                                let doc_fk_cabecera = fk_cabecera;
                                let doc_seq_num = auto_num;
                                let doc_analysis_text = resp_cust_code;
                                let doc_tran_date = pago.fecha;
                                let doc_ref_text = resp_cust_code;
                                let doc_ref_num = (documentos[i].resp_inv_num==''||documentos[i].resp_inv_num==null||documentos[i].resp_inv_num==undefined)?0:documentos[i].resp_inv_num;
                                let doc_debit_amt = 0;
                                let doc_credit_amt = curr_pay;
                                let doc_debit_amt1 = 0;
                                let doc_credit_amt1 = 0;
                                let doc_debit_amt2 = 0;
                                let doc_credit_amt2 = 0;
                                let doc_debit_amt3 = 0;
                                let doc_credit_amt3 = 0;
                                let doc_ref_text1 = pago.n_carpeta;
                                let doc_bran_code = '1';
                                let doc_profit_code = 'CM';
                                let doc_currency_code = 'CLP';
                                let doc_rate_exchange = 1;
                                let doc_desc_text = razon_social;

                                let doc_acct_code = '11301001-001';
                                let doc_ref_text2 = 'PAGO DIN WSC';

                                if (documentos[i].resp_doc_code != 'DI') {
                                    doc_acct_code = '11301001-000';
                                    doc_ref_text2 = 'PAGO FACT';
                                }

                                if (documentos[i].resp_doc_code == 'PALLET' || documentos[i].resp_doc_code == 'TVP' || documentos[i].resp_doc_code == 'OTROS') {
                                    doc_acct_code = '21501002-000';
                                    doc_ref_text2 = documentos[i].resp_doc_code;
                                    crear_pago_wsc_cws(xml_cws, pago, resp_cust_code, curr_pay, documentos[i].resp_doc_code);
                                }

                                columna = '';                           valor = '';
                                columna+=`"fk_cabecera",`;              valor+=`'`+ doc_fk_cabecera +`',`;
                                columna+=`"seq_num",`;                  valor+=`'`+ doc_seq_num +`',`;
                                columna+=`"analysis_text",`;            valor+=`'`+ doc_analysis_text +`',`;
                                columna+=`"tran_date",`;                valor+=`'`+ doc_tran_date +`',`;
                                columna+=`"ref_text",`;                 valor+=`'`+ doc_ref_text +`',`;
                                columna+=`"ref_num",`;                  valor+=`'`+ doc_ref_num +`',`;
                                columna+=`"debit_amt",`;                valor+=`'`+ doc_debit_amt +`',`;
                                columna+=`"credit_amt",`;               valor+=`'`+ doc_credit_amt +`',`;
                                columna+=`"debit_amt1",`;               valor+=`'`+ doc_debit_amt1 +`',`;
                                columna+=`"credit_amt1",`;              valor+=`'`+ doc_credit_amt1 +`',`;
                                columna+=`"debit_amt2",`;               valor+=`'`+ doc_debit_amt2 +`',`;
                                columna+=`"credit_amt2",`;              valor+=`'`+ doc_credit_amt2 +`',`;
                                columna+=`"debit_amt3",`;               valor+=`'`+ doc_debit_amt3 +`',`;
                                columna+=`"credit_amt3",`;              valor+=`'`+ doc_credit_amt3 +`',`;
                                columna+=`"ref_text1",`;                valor+=`'`+ doc_ref_text1 +`',`;
                                columna+=`"bran_code",`;                valor+=`'`+ doc_bran_code +`',`;
                                columna+=`"profit_code",`;              valor+=`'`+ doc_profit_code +`',`;
                                columna+=`"currency_code",`;            valor+=`'`+ doc_currency_code +`',`;
                                columna+=`"rate_exchange",`;            valor+=`'`+ doc_rate_exchange +`',`;
                                columna+=`"acct_code",`;                valor+=`'`+ doc_acct_code +`',`;
                                columna+=`"ref_text2",`;                valor+=`'`+ doc_ref_text2 +`',`;
                                columna+=`"desc_text"`;                 valor+=`'`+ doc_desc_text +`'`;

                                save_detalle(columna, valor);
                                /** XML DETALLE DOCUMENTO **/
                                xmltext_aux += `<DETAIL>`;
                                xmltext_aux += `<CMPY_CODE>`+ '04' +`</CMPY_CODE>`;
                                xmltext_aux += `<SEQ_NUM>`+ doc_seq_num +`</SEQ_NUM>`;
                                xmltext_aux += `<TRAN_TYPE_IND>`+ 'CPA' +`</TRAN_TYPE_IND>`;
                                xmltext_aux += `<ANALYSIS_TEXT>`+ doc_analysis_text +`</ANALYSIS_TEXT>`;
                                xmltext_aux += `<TRAN_DATE>`+ doc_tran_date +`</TRAN_DATE>`;
                                xmltext_aux += `<REF_TEXT>`+ doc_ref_text +`</REF_TEXT>`;
                                xmltext_aux += `<REF_NUM>`+ doc_ref_num +`</REF_NUM>`;
                                xmltext_aux += `<ACCT_CODE>`+ doc_acct_code +`</ACCT_CODE>`;
                                xmltext_aux += `<DEBIT_AMT>`+ doc_debit_amt +`</DEBIT_AMT>`;
                                xmltext_aux += `<CREDIT_AMT>`+ doc_credit_amt +`</CREDIT_AMT>`;
                                xmltext_aux += `<DEBIT_AMT1>`+ doc_debit_amt1 +`</DEBIT_AMT1>`;
                                xmltext_aux += `<CREDIT_AMT1>`+ doc_credit_amt1 +`</CREDIT_AMT1>`;
                                xmltext_aux += `<DEBIT_AMT2>`+ doc_debit_amt2 +`</DEBIT_AMT2>`;
                                xmltext_aux += `<CREDIT_AMT2>`+ doc_credit_amt2 +`</CREDIT_AMT2>`;
                                xmltext_aux += `<REF_TEXT1>`+ doc_ref_text1 +`</REF_TEXT1>`;
                                xmltext_aux += `<REF_TEXT2>`+ doc_ref_text2 +`</REF_TEXT2>`;
                                xmltext_aux += `<DESC_TEXT>`+ doc_desc_text.replace('&', 'y').replace('&', 'y') +`</DESC_TEXT>`;
                                xmltext_aux += `<BRAN_CODE>`+ doc_bran_code +`</BRAN_CODE>`;
                                xmltext_aux += `<PROFIT_CODE>`+ doc_profit_code +`</PROFIT_CODE>`;
                                xmltext_aux += `<CURRENCY_CODE>`+ doc_currency_code +`</CURRENCY_CODE>`;
                                xmltext_aux += `<RATE_EXCHANGE>`+ doc_rate_exchange +`</RATE_EXCHANGE>`;
                                xmltext_aux += `<DEBIT_AMT3>`+ doc_debit_amt3 +`</DEBIT_AMT3>`;
                                xmltext_aux += `<CREDIT_AMT3>`+ doc_credit_amt3 +`</CREDIT_AMT3>`;
                                xmltext_aux += `<REF_AMT>`+ doc_credit_amt +`</REF_AMT>`;

                                xmltext_aux += `<CURRENCY_IND >`+ '1' +`</CURRENCY_IND >`;

                                xmltext_aux += `</DETAIL>`;

                                if (payment == 0) {
                                    break;
                                }
                                auto_num++;
                            }

                            if (payment > 0) {//SOBRO DINERO, SE AGREGA A AJUSTE

                                let aju_fk_cabecera = fk_cabecera;
                                let aju_seq_num = auto_num;
                                let aju_analysis_text = resp_cust_code;
                                let aju_tran_date = pago.fecha;
                                let aju_ref_text = resp_cust_code;
                                let aju_ref_num = 0;
                                let aju_debit_amt = 0;
                                let aju_credit_amt = payment;
                                let aju_debit_amt1 = 0;
                                let aju_credit_amt1 = 0;
                                let aju_debit_amt2 = 0;
                                let aju_credit_amt2 = 0;
                                let aju_debit_amt3 = 0;
                                let aju_credit_amt3 = 0;
                                let aju_ref_text1 = pago.n_carpeta;
                                let aju_bran_code = '';
                                let aju_profit_code = '';
                                let aju_currency_code = 'CLP';
                                let aju_rate_exchange = 1;

                                let aju_acct_code = '21102001-000';
                                let aju_ref_text2 = 'EXCEDENTE CLIENTE';
                                let aju_desc_text = razon_social;

                                columna = '';                           valor = '';
                                columna+=`"fk_cabecera",`;              valor+=`'`+ aju_fk_cabecera +`',`;
                                columna+=`"seq_num",`;                  valor+=`'`+ aju_seq_num +`',`;
                                columna+=`"analysis_text",`;            valor+=`'`+ aju_analysis_text +`',`;
                                columna+=`"tran_date",`;                valor+=`'`+ aju_tran_date +`',`;
                                columna+=`"ref_text",`;                 valor+=`'`+ aju_ref_text +`',`;
                                columna+=`"ref_num",`;                  valor+=`'`+ aju_ref_num +`',`;
                                columna+=`"debit_amt",`;                valor+=`'`+ aju_debit_amt +`',`;
                                columna+=`"credit_amt",`;               valor+=`'`+ aju_credit_amt +`',`;
                                columna+=`"debit_amt1",`;               valor+=`'`+ aju_debit_amt1 +`',`;
                                columna+=`"credit_amt1",`;              valor+=`'`+ aju_credit_amt1 +`',`;
                                columna+=`"debit_amt2",`;               valor+=`'`+ aju_debit_amt2 +`',`;
                                columna+=`"credit_amt2",`;              valor+=`'`+ aju_credit_amt2 +`',`;
                                columna+=`"debit_amt3",`;               valor+=`'`+ aju_debit_amt3 +`',`;
                                columna+=`"credit_amt3",`;              valor+=`'`+ aju_credit_amt3 +`',`;
                                columna+=`"ref_text1",`;                valor+=`'`+ aju_ref_text1 +`',`;
                                columna+=`"bran_code",`;                valor+=`'`+ aju_bran_code +`',`;
                                columna+=`"profit_code",`;              valor+=`'`+ aju_profit_code +`',`;
                                columna+=`"currency_code",`;            valor+=`'`+ aju_currency_code +`',`;
                                columna+=`"rate_exchange",`;            valor+=`'`+ aju_rate_exchange +`',`;
                                columna+=`"acct_code",`;                valor+=`'`+ aju_acct_code +`',`;
                                columna+=`"ref_text2",`;                valor+=`'`+ aju_ref_text2 +`',`;
                                columna+=`"desc_text"`;                 valor+=`'`+ aju_desc_text +`'`;

                                save_detalle(columna, valor);
                                /** XML DETALLE BANCO  **/
                                xmltext_aux += `<DETAIL>`;
                                xmltext_aux += `<CMPY_CODE>`+ '04' +`</CMPY_CODE>`;
                                xmltext_aux += `<SEQ_NUM>`+ aju_seq_num +`</SEQ_NUM>`;
                                xmltext_aux += `<TRAN_TYPE_IND>`+ 'AJU' +`</TRAN_TYPE_IND>`;
                                xmltext_aux += `<ANALYSIS_TEXT>`+ aju_analysis_text +`</ANALYSIS_TEXT>`;
                                xmltext_aux += `<TRAN_DATE>`+ aju_tran_date +`</TRAN_DATE>`;
                                xmltext_aux += `<REF_TEXT>`+ aju_ref_text +`</REF_TEXT>`;
                                xmltext_aux += `<REF_NUM>`+ aju_ref_num +`</REF_NUM>`;
                                xmltext_aux += `<ACCT_CODE>`+ aju_acct_code +`</ACCT_CODE>`;
                                xmltext_aux += `<DEBIT_AMT>`+ aju_debit_amt +`</DEBIT_AMT>`;
                                xmltext_aux += `<CREDIT_AMT>`+ aju_credit_amt +`</CREDIT_AMT>`;
                                xmltext_aux += `<DEBIT_AMT1>`+ aju_debit_amt1 +`</DEBIT_AMT1>`;
                                xmltext_aux += `<CREDIT_AMT1>`+ aju_credit_amt1 +`</CREDIT_AMT1>`;
                                xmltext_aux += `<DEBIT_AMT2>`+ aju_debit_amt2 +`</DEBIT_AMT2>`;
                                xmltext_aux += `<CREDIT_AMT2>`+ aju_credit_amt2 +`</CREDIT_AMT2>`;
                                xmltext_aux += `<REF_TEXT1>`+ aju_ref_text1 +`</REF_TEXT1>`;
                                xmltext_aux += `<REF_TEXT2>`+ aju_ref_text2 +`</REF_TEXT2>`;
                                xmltext_aux += `<DESC_TEXT>`+ aju_desc_text.replace('&', 'y').replace('&', 'y') +`</DESC_TEXT>`;
                                xmltext_aux += `<BRAN_CODE>`+ aju_bran_code +`</BRAN_CODE>`;
                                xmltext_aux += `<PROFIT_CODE>`+ aju_profit_code +`</PROFIT_CODE>`;
                                xmltext_aux += `<CURRENCY_CODE>`+ aju_currency_code +`</CURRENCY_CODE>`;
                                xmltext_aux += `<RATE_EXCHANGE>`+ aju_rate_exchange +`</RATE_EXCHANGE>`;
                                xmltext_aux += `<DEBIT_AMT3>`+ aju_debit_amt3 +`</DEBIT_AMT3>`;
                                xmltext_aux += `<CREDIT_AMT3>`+ aju_credit_amt3 +`</CREDIT_AMT3>`;

                                xmltext_aux += `<CURRENCY_IND >`+ '1' +`</CURRENCY_IND >`;

                                xmltext_aux += `</DETAIL>`;
                            }

                            xmltext_aux +=`</BATCH></string>`;
                            send_pago_maximise(xmltext_aux, fk_cabecera)

                        }
                    }
                });
            } catch (error) {
                console.log("ERROR "+error);

            }
        }

        async function get_cliente_rut(fk_cliente) {

            var rut_cliente = await client.query(`SELECT rut FROM public.clientes WHERE id='${fk_cliente}'`);
            if (rut_cliente.rows.length > 0) {
                return rut_cliente.rows[0]['rut'].replace('.', '').replace('.', '');
            } else {
                return false
            }

        }

        async function save_detalle(columna, valor) {

            console.log(` INSERT INTO public.wsc_envio_asientos_detalles (`+columna+`) VALUES (`+valor+`) RETURNING *`);
            var insert = await client.query(` INSERT INTO public.wsc_envio_asientos_detalles (`+columna+`) VALUES (`+valor+`) RETURNING *`);

        }

        async function crear_pago_wsc_cws(xmlBase, pago, cust_code, monto, concepto) {

            //          DEBE
            xmlBase += `<DETAIL>`;
            xmlBase += `<CMPY_CODE>`+ '03' +`</CMPY_CODE>`;
            xmlBase += `<SEQ_NUM>`+ 1 +`</SEQ_NUM>`;
            xmlBase += `<TRAN_TYPE_IND>`+ 'AJU' +`</TRAN_TYPE_IND>`;
            xmlBase += `<ANALYSIS_TEXT>`+ '' +`</ANALYSIS_TEXT>`;
            xmlBase += `<TRAN_DATE>`+ pago.fecha +`</TRAN_DATE>`;
            xmlBase += `<REF_TEXT>`+ cust_code +`</REF_TEXT>`;
            xmlBase += `<REF_NUM>`+ 0 +`</REF_NUM>`;
            xmlBase += `<ACCT_CODE>`+ '11301004-000' +`</ACCT_CODE>`;
            xmlBase += `<DEBIT_AMT>`+ monto +`</DEBIT_AMT>`;
            xmlBase += `<CREDIT_AMT>`+ 0 +`</CREDIT_AMT>`;
            xmlBase += `<DEBIT_AMT1>`+ 0 +`</DEBIT_AMT1>`;
            xmlBase += `<CREDIT_AMT1>`+ 0 +`</CREDIT_AMT1>`;
            xmlBase += `<DEBIT_AMT2>`+ 0 +`</DEBIT_AMT2>`;
            xmlBase += `<CREDIT_AMT2>`+ 0 +`</CREDIT_AMT2>`;
            xmlBase += `<REF_TEXT1>`+ pago.n_carpeta +`</REF_TEXT1>`;
            xmlBase += `<REF_TEXT2>`+ cust_code +`</REF_TEXT2>`;
            xmlBase += `<DESC_TEXT>`+ 'CUENTA CORRIENTE WSCARGO' +`</DESC_TEXT>`;
            xmlBase += `<BRAN_CODE>`+ '' +`</BRAN_CODE>`;
            xmlBase += `<PROFIT_CODE>`+ '' +`</PROFIT_CODE>`;
            xmlBase += `<CURRENCY_CODE>`+ 'CLP' +`</CURRENCY_CODE>`;
            xmlBase += `<RATE_EXCHANGE>`+ 1 +`</RATE_EXCHANGE>`;
            xmlBase += `<DEBIT_AMT3>`+ 0 +`</DEBIT_AMT3>`;
            xmlBase += `<CREDIT_AMT3>`+ 0 +`</CREDIT_AMT3>`;
            xmlBase += `<CURRENCY_IND >`+ '1' +`</CURRENCY_IND >`;
            xmlBase += `</DETAIL>`;

            //          HABER
            xmlBase += `<DETAIL>`;
            xmlBase += `<CMPY_CODE>`+ '03' +`</CMPY_CODE>`;
            xmlBase += `<SEQ_NUM>`+ 1 +`</SEQ_NUM>`;
            xmlBase += `<TRAN_TYPE_IND>`+ 'AJU' +`</TRAN_TYPE_IND>`;
            xmlBase += `<ANALYSIS_TEXT>`+ '' +`</ANALYSIS_TEXT>`;
            xmlBase += `<TRAN_DATE>`+ pago.fecha +`</TRAN_DATE>`;
            xmlBase += `<REF_TEXT>`+ cust_code +`</REF_TEXT>`;
            xmlBase += `<REF_NUM>`+ 0 +`</REF_NUM>`;
            xmlBase += `<ACCT_CODE>`+ '11301001-000' +`</ACCT_CODE>`;
            xmlBase += `<DEBIT_AMT>`+ 0 +`</DEBIT_AMT>`;
            xmlBase += `<CREDIT_AMT>`+ monto +`</CREDIT_AMT>`;
            xmlBase += `<DEBIT_AMT1>`+ 0 +`</DEBIT_AMT1>`;
            xmlBase += `<CREDIT_AMT1>`+ 0 +`</CREDIT_AMT1>`;
            xmlBase += `<DEBIT_AMT2>`+ 0 +`</DEBIT_AMT2>`;
            xmlBase += `<CREDIT_AMT2>`+ 0 +`</CREDIT_AMT2>`;
            xmlBase += `<REF_TEXT1>`+ pago.n_carpeta +`</REF_TEXT1>`;
            xmlBase += `<REF_TEXT2>`+ concepto +`</REF_TEXT2>`;
            xmlBase += `<DESC_TEXT>`+ 'CLIENTES NACIONALES ' +`</DESC_TEXT>`;
            xmlBase += `<BRAN_CODE>`+ '' +`</BRAN_CODE>`;
            xmlBase += `<PROFIT_CODE>`+ '' +`</PROFIT_CODE>`;
            xmlBase += `<CURRENCY_CODE>`+ 'CLP' +`</CURRENCY_CODE>`;
            xmlBase += `<RATE_EXCHANGE>`+ 1 +`</RATE_EXCHANGE>`;
            xmlBase += `<DEBIT_AMT3>`+ 0 +`</DEBIT_AMT3>`;
            xmlBase += `<CREDIT_AMT3>`+ 0 +`</CREDIT_AMT3>`;
            xmlBase += `<CURRENCY_IND >`+ '1' +`</CURRENCY_IND >`;
            xmlBase += `</DETAIL>`;

            xmlBase +=`</BATCH></string>`;

            send_pago_maximise(xmlBase, -1)

        }

        async function send_pago_maximise(xml, fk_cabecera) {

            console.log(xml);

            const request = require('request');
            const fs = require("fs");

            const data = {
                AliasName: 'dwi_tnm',
                UserName: 'webservice',
                Password: 'webservice001',
                Data: xml
            };

            const options = {
                url: 'http://asp3.maximise.cl/wsv/batch.asmx/SaveDocument',
                method: 'POST',
                headers: {
                    'Content-Type': 'text/html',
                    'Content-Length': data.length
                },
                json: true,
                form: {
                    AliasName: 'dwi_tnm',
                    UserName: 'webservice',
                    Password: 'webservice001',
                    Data: xml
                }
            };

            request.post(options, (err, res, body) => {

                if(err) {
                    console.log('error, ' + err);
                }
                else if(body) {
                    console.log(" RESPUESTA ");
                    if(res.statusCode!=200 ){

                        console.log("ERROR ");
                        console.log(JSON.stringify(res.body));

                    } else if(res.statusCode==200 ) {

                        console.log("\n\nSUCCESS ");
                        console.log("\n\n"+JSON.stringify(res.body));
                        var RespMax = JSON.stringify(res.body);
                        var IdMax = RespMax.match(/<int xmlns=\"Maximise\">([^<]*)<\/int>/);
                        console.log('\n\nId Maximise '+IdMax);
                        if (fk_cabecera != -1)
                        {
                            actualizar_estado_bd('MAXIMISE', fk_cabecera, IdMax)
                        }
                    }
                }
            });

        }

        async function actualizar_estado_bd(estado, fk_cabecera, IdMax) {

            var moment = require('moment');
            let fecha = moment(Date.now()).format("YYYY-MM-DD HH:mm:ss");

            var query = '';
                query+=`estado='`+estado+`', `;
                query+=`id_maximise='`+IdMax+`', `;
                query+=`"fk_updatedBy"=`+carpeta_data.rows[0]["fk_createdBy"]+`, `;
                query+=`"updatedAt"='`+fecha+`'`;

            console.log(`UPDATE public.wsc_envio_asientos_cabeceras SET `+query+` WHERE id=`+fk_cabecera+` RETURNING *`);
            await client.query(`UPDATE public.wsc_envio_asientos_cabeceras SET `+query+` WHERE id=`+fk_cabecera+` RETURNING *`);

            client.query(` UPDATE public.queue_maximise SET estado='${estado}' WHERE id=${tarea.id} `);

        }
    }

    async function enviar_pago_usd_maximise(tarea) {
        console.log('Envio de pago usd')
        client.query(` UPDATE public.queue_maximise SET estado='PROCESANDO' WHERE id=${tarea.id} `);

        var carpeta_data = await client.query(` SELECT id, fk_responsable, errores, estado, cmpy_code, year_num, period_num, jour_code, entry_code, debit_amt, credit_amt, post_flag, com_text, cleared_flag, control_amt, debit_amt1, credit_amt1, debit_amt2, credit_amt2, debit_amt3, credit_amt3, tran_type_ind, ledger_code, fk_despacho, fk_nota_cobro, carpeta, codigo_cliente, monto, descripcion_movimiento, saldo, fecha_ingreso, fecha_registro, "fk_createdBy", "fk_updatedBy", "createdAt", "updatedAt", codigo_bi_pago, conversion, razon_social,
                                                    to_char(entry_date, 'DD-MM-YYYY') as entry_date,
                                                    to_char(jour_date, 'DD-MM-YYYY') as jour_date,
                                                    to_char(fecha_movimiento, 'DD-MM-YYYY') as fecha_movimiento
                                                FROM public.wsc_envio_asientos_cabeceras where id=${tarea['fk_cabecera']}`);

        if(carpeta_data.rows.length>0) {

            let cmpy_code = carpeta_data.rows[0].cmpy_code;
            let year_num = carpeta_data.rows[0].year_num;
            let period_num = carpeta_data.rows[0].period_num;
            let jour_code = carpeta_data.rows[0].jour_code;
            let entry_code = carpeta_data.rows[0].entry_code;
            let entry_date = carpeta_data.rows[0].entry_date;
            let jour_date = carpeta_data.rows[0].jour_date;
            let debit_amt = carpeta_data.rows[0].debit_amt;
            let credit_amt = carpeta_data.rows[0].credit_amt;
            let post_flag = carpeta_data.rows[0].post_flag;
            let com_text = carpeta_data.rows[0].com_text;
            let cleared_flag = carpeta_data.rows[0].cleared_flag;
            let control_amt = carpeta_data.rows[0].control_amt;
            let debit_amt1 = carpeta_data.rows[0].debit_amt1;
            let credit_amt1 = carpeta_data.rows[0].credit_amt1;
            let debit_amt2 = carpeta_data.rows[0].debit_amt2;
            let credit_amt2 = carpeta_data.rows[0].credit_amt2;
            let tran_type_ind = carpeta_data.rows[0].tran_type_ind;
            let ledger_code = carpeta_data.rows[0].ledger_code;
            let debit_amt3 = carpeta_data.rows[0].debit_amt3;
            let credit_amt3 = carpeta_data.rows[0].credit_amt3;
            let rate_exchange = carpeta_data.rows[0].conversion;
            let razon_social = carpeta_data.rows[0].razon_social;

            let carpeta = carpeta_data.rows[0].carpeta;
            let monto = carpeta_data.rows[0].monto;
            let codigo_cliente = carpeta_data.rows[0].codigo_cliente;
            let fecha_movimiento = carpeta_data.rows[0].fecha_movimiento;

            var xmltext_aux = `<string xmlns="Maximise"><BATCH><HEAD>`;
            xmltext_aux += `<CMPY_CODE>`+ cmpy_code +`</CMPY_CODE>`;
            xmltext_aux += `<YEAR_NUM>`+ year_num +`</YEAR_NUM>`;
            xmltext_aux += `<PERIOD_NUM>`+ period_num +`</PERIOD_NUM>`;
            xmltext_aux += `<JOUR_CODE>`+ jour_code +`</JOUR_CODE>`;
            xmltext_aux += `<ENTRY_CODE>`+ entry_code +`</ENTRY_CODE>`;
            xmltext_aux += `<ENTRY_DATE>`+ entry_date +`</ENTRY_DATE>`;
            xmltext_aux += `<JOUR_DATE>`+ jour_date +`</JOUR_DATE>`;
            xmltext_aux += `<DEBIT_AMT>`+ debit_amt +`</DEBIT_AMT>`;
            xmltext_aux += `<CREDIT_AMT>`+ credit_amt +`</CREDIT_AMT>`;
            xmltext_aux += `<POST_FLAG>`+ post_flag +`</POST_FLAG>`;
            xmltext_aux += `<COM_TEXT>`+ com_text +`</COM_TEXT>`;
            xmltext_aux += `<CLEARED_FLAG>`+ cleared_flag +`</CLEARED_FLAG>`;
            xmltext_aux += `<CONTROL_AMT>`+ control_amt +`</CONTROL_AMT>`;
            xmltext_aux += `<DEBIT_AMT1>`+ debit_amt1 +`</DEBIT_AMT1>`;
            xmltext_aux += `<CREDIT_AMT1>`+ credit_amt1 +`</CREDIT_AMT1>`;
            xmltext_aux += `<DEBIT_AMT2>`+ debit_amt2 +`</DEBIT_AMT2>`;
            xmltext_aux += `<CREDIT_AMT2>`+ credit_amt2 +`</CREDIT_AMT2>`;
            xmltext_aux += `<TRAN_TYPE_IND>`+ tran_type_ind +`</TRAN_TYPE_IND>`;
            xmltext_aux += `<LEDGER_CODE>`+ ledger_code +`</LEDGER_CODE>`;
            xmltext_aux += `<DEBIT_AMT3>`+ debit_amt3 +`</DEBIT_AMT3>`;
            xmltext_aux += `<CREDIT_AMT3>`+ credit_amt3 +`</CREDIT_AMT3>`;
            xmltext_aux += `<CURRENCY_IND >`+ '1' +`</CURRENCY_IND >`;
            xmltext_aux += `<RATE_EXCHANGE >`+ rate_exchange +`</RATE_EXCHANGE >`;
            xmltext_aux += `</HEAD>`;

            let pago = {
                n_carpeta: carpeta,
                monto: monto,
                cliente: codigo_cliente,
                fecha: fecha_movimiento,
                conversion: rate_exchange
            }

            consultar_carpeta_maximise(tarea['fk_cabecera'], pago, xmltext_aux, razon_social);

        }

        async function consultar_carpeta_maximise(fk_cabecera, pago, xmltext_aux, razon_social) {

            const request = require('request');
            const fs = require("fs");

            const xmltext = `SELECT ih.INV_NUM, ih.doc_code, ih.cust_code, ih.PAID_AMT, ih.TOTAL_AMT,ih.REF_TEXT1,ih.REF_TEXT2, ih.ext_num,
                            (SELECT top 1 TOTAL_AMT FROM credithead ch WHERE ch.inv_num = ih.inv_num AND POSTED_FLAG<>'V' AND (CMPY_CODE='02' OR CMPY_CODE='03' OR CMPY_CODE='04')) as total_credito
                            FROM invoicehead ih
                            WHERE ih.cmpy_code='04' and
                            ih.POSTED_FLAG<>'V' and
                            ih.ref_text1='${pago.n_carpeta}'
                            ORDER BY ih.doc_code asc;`;

            const data = {
                AliasName: 'dwi_tnm',
                UserName: 'webservice',
                Password: 'webservice001',
                Sql: xmltext
            };

            const options = {
                url: 'http://asp3.maximise.cl/wsv/query.asmx/GetQueryAsDataSet',
                method: 'POST',
                headers: {
                    'Content-Type': 'text/html',
                    'Content-Length': data.length
                },
                json: true,
                form: {
                    AliasName: 'dwi_tnm',
                    UserName: 'webservice',
                    Password: 'webservice001',
                    Sql: xmltext
                }
            };

            try {

                request.post(options, async(err, res, body) => {

                    if(err)
                    {
                        console.log(err);
                    }
                    else if(body)
                    {
                        console.log(" RESPONSE GET INVOICEHEAD ");
                        if(res.statusCode!=200){

                            console.log("ERROR");
                            console.log(JSON.stringify(res.body));

                        }
                        else if(res.statusCode==200 )
                        {
                            var documentos = [];
                            var payment = Number(pago.monto) * Number(pago.conversion);

                            var entries = JSON.stringify(body).split('<Table');

                            let resp_cust_code = '';
                            let nota_debito = 0;

                            //GUARDO DOCUMENTOS EN ARRAY DE OBJETOS
                            for(var j=1; j<entries.length; j++) {
                                let resp_inv_num = entries[j].split('<INV_NUM>').pop().split('</INV_NUM>')[0];
                                let resp_doc_code = entries[j].split('<doc_code>').pop().split('</doc_code>')[0];
                                resp_cust_code = entries[j].split('<cust_code>').pop().split('</cust_code>')[0];
                                let resp_paid_amt = entries[j].split('<PAID_AMT>').pop().split('</PAID_AMT>')[0];
                                let resp_total_amt = entries[j].split('<TOTAL_AMT>').pop().split('</TOTAL_AMT>')[0];
                                let resp_ref_text1 = entries[j].split('<REF_TEXT1>').pop().split('</REF_TEXT1>')[0];
                                let resp_ref_text2 = entries[j].split('<REF_TEXT2>').pop().split('</REF_TEXT2>')[0];
                                let resp_ext_num = entries[j].split('<ext_num>').pop().split('</ext_num>')[0];
                                let resp_total_credito = entries[j].split('<total_credito>').pop().split('</total_credito>')[0];
                                let resp_cred_num = entries[j].split('<cred_num>').pop().split('</cred_num>')[0];

                                let resp_por_pagar = Number(resp_total_amt) - Number(resp_paid_amt);

                                try {
                                    if (Number(resp_total_credito) > 0) {
                                        resp_por_pagar -= Number(resp_total_credito)
                                    }
                                } catch {

                                }

                                if (resp_doc_code == 'F8') {
                                    resp_por_pagar += nota_debito;
                                }

                                if (resp_doc_code == 'D1'){
                                    nota_debito = Number(resp_total_amt)

                                } else if (resp_doc_code != 'P8' && resp_por_pagar > 10) {
                                    documentos.push({
                                        resp_inv_num: resp_inv_num,
                                        resp_doc_code: resp_doc_code,
                                        resp_paid_amt: resp_paid_amt,
                                        resp_total_amt: resp_total_amt,
                                        resp_por_pagar: resp_por_pagar,
                                        resp_ref_text1: resp_ref_text1,
                                        resp_ref_text2: resp_ref_text2,
                                        resp_ext_num: resp_ext_num
                                    })
                                }
                            }
                            console.log(documentos);

                            if (documentos.length == 0) {
                                //DINERO SE IRA A EXCEDENTE, PERO HAY CAMPOS QUE DEBO CONSULTAR DESDE OTRA FUENTE
                                resp_cust_code = await get_cliente_rut(pago.cliente.split(' ')[0]);
                                if (resp_cust_code == false) {
                                    return false;
                                }
                            }

                            // BANCO BANCO BANCO BANCO BANCO BANCO BANCO BANCO BANCO

                            var auto_num = 1;

                            let bco_fk_cabecera = fk_cabecera;
                            let bco_seq_num = auto_num;
                            let bco_analysis_text = '';
                            let bco_tran_date = pago.fecha;
                            let bco_ref_text = resp_cust_code;
                            let bco_ref_num = 0;
                            let bco_debit_amt = payment;
                            let bco_credit_amt = 0;
                            let bco_debit_amt1 = payment/Number(pago.conversion);
                            let bco_credit_amt1 = 0;
                            let bco_debit_amt2 = 0;
                            let bco_credit_amt2 = 0;
                            let bco_debit_amt3 = 0;
                            let bco_credit_amt3 = 0;
                            let bco_ref_text1 = pago.n_carpeta;
                            let bco_bran_code = '';
                            let bco_profit_code = '';
                            let bco_currency_code = 'USD';
                            let bco_rate_exchange = 1;

                            let bco_acct_code = '11103002-000';
                            let bco_ref_text2 = resp_cust_code;
                            let bco_desc_text = 'BANCO SANTANDER';

                            columna = '';                           valor = '';
                            columna+=`"fk_cabecera",`;              valor+=`'`+ bco_fk_cabecera +`',`;
                            columna+=`"seq_num",`;                  valor+=`'`+ bco_seq_num +`',`;
                            columna+=`"analysis_text",`;            valor+=`'`+ bco_analysis_text +`',`;
                            columna+=`"tran_date",`;                valor+=`'`+ bco_tran_date +`',`;
                            columna+=`"ref_text",`;                 valor+=`'`+ bco_ref_text +`',`;
                            columna+=`"ref_num",`;                  valor+=`'`+ bco_ref_num +`',`;
                            columna+=`"debit_amt",`;                valor+=`'`+ bco_debit_amt +`',`;
                            columna+=`"credit_amt",`;               valor+=`'`+ bco_credit_amt +`',`;
                            columna+=`"debit_amt1",`;               valor+=`'`+ bco_debit_amt1 +`',`;
                            columna+=`"credit_amt1",`;              valor+=`'`+ bco_credit_amt1 +`',`;
                            columna+=`"debit_amt2",`;               valor+=`'`+ bco_debit_amt2 +`',`;
                            columna+=`"credit_amt2",`;              valor+=`'`+ bco_credit_amt2 +`',`;
                            columna+=`"debit_amt3",`;               valor+=`'`+ bco_debit_amt3 +`',`;
                            columna+=`"credit_amt3",`;              valor+=`'`+ bco_credit_amt3 +`',`;
                            columna+=`"ref_text1",`;                valor+=`'`+ bco_ref_text1 +`',`;
                            columna+=`"bran_code",`;                valor+=`'`+ bco_bran_code +`',`;
                            columna+=`"profit_code",`;              valor+=`'`+ bco_profit_code +`',`;
                            columna+=`"currency_code",`;            valor+=`'`+ bco_currency_code +`',`;
                            columna+=`"rate_exchange",`;            valor+=`'`+ bco_rate_exchange +`',`;
                            columna+=`"acct_code",`;                valor+=`'`+ bco_acct_code +`',`;
                            columna+=`"ref_text2",`;                valor+=`'`+ bco_ref_text2 +`',`;
                            columna+=`"desc_text"`;                 valor+=`'`+ bco_desc_text +`'`;

                            save_detalle(columna, valor);
                            /** XML DETALLE BANCO  **/
                            xmltext_aux += `<DETAIL>`;
                            xmltext_aux += `<CMPY_CODE>`+ '04' +`</CMPY_CODE>`;
                            xmltext_aux += `<SEQ_NUM>`+ bco_seq_num +`</SEQ_NUM>`;
                            xmltext_aux += `<TRAN_TYPE_IND>`+ 'AJU' +`</TRAN_TYPE_IND>`;
                            xmltext_aux += `<ANALYSIS_TEXT>`+ bco_analysis_text +`</ANALYSIS_TEXT>`;
                            xmltext_aux += `<TRAN_DATE>`+ bco_tran_date +`</TRAN_DATE>`;
                            xmltext_aux += `<REF_TEXT>`+ bco_ref_text +`</REF_TEXT>`;
                            xmltext_aux += `<REF_NUM>`+ bco_ref_num +`</REF_NUM>`;
                            xmltext_aux += `<ACCT_CODE>`+ bco_acct_code +`</ACCT_CODE>`;
                            xmltext_aux += `<DEBIT_AMT>`+ bco_debit_amt +`</DEBIT_AMT>`;
                            xmltext_aux += `<CREDIT_AMT>`+ bco_credit_amt +`</CREDIT_AMT>`;
                            xmltext_aux += `<DEBIT_AMT1>`+ bco_debit_amt1 +`</DEBIT_AMT1>`;
                            xmltext_aux += `<CREDIT_AMT1>`+ bco_credit_amt1 +`</CREDIT_AMT1>`;
                            xmltext_aux += `<DEBIT_AMT2>`+ bco_debit_amt2 +`</DEBIT_AMT2>`;
                            xmltext_aux += `<CREDIT_AMT2>`+ bco_credit_amt2 +`</CREDIT_AMT2>`;
                            xmltext_aux += `<REF_TEXT1>`+ bco_ref_text1 +`</REF_TEXT1>`;
                            xmltext_aux += `<REF_TEXT2>`+ bco_ref_text2 +`</REF_TEXT2>`;
                            xmltext_aux += `<DESC_TEXT>`+ bco_desc_text +`</DESC_TEXT>`;
                            xmltext_aux += `<BRAN_CODE>`+ bco_bran_code +`</BRAN_CODE>`;
                            xmltext_aux += `<PROFIT_CODE>`+ bco_profit_code +`</PROFIT_CODE>`;
                            xmltext_aux += `<CURRENCY_CODE>`+ bco_currency_code +`</CURRENCY_CODE>`;
                            xmltext_aux += `<RATE_EXCHANGE>`+ bco_rate_exchange +`</RATE_EXCHANGE>`;
                            xmltext_aux += `<DEBIT_AMT3>`+ bco_debit_amt3 +`</DEBIT_AMT3>`;
                            xmltext_aux += `<CREDIT_AMT3>`+ bco_credit_amt3 +`</CREDIT_AMT3>`;
                            xmltext_aux += `<JOUR_CODE >`+ 'IPC' +`</JOUR_CODE >`;

                            xmltext_aux += `</DETAIL>`;

                            auto_num++;
                            // DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO DOCUMENTO
                            for (let i=0; i<documentos.length; i++) {

                                console.log(documentos[i].resp_inv_num);

                                var curr_pay = 0;
                                if(payment - documentos[i].resp_por_pagar >= 0) {
                                    curr_pay = documentos[i].resp_por_pagar;
                                    payment -= documentos[i].resp_por_pagar;
                                } else {
                                    curr_pay = payment;
                                    payment = 0;
                                }

                                let doc_fk_cabecera = fk_cabecera;
                                let doc_seq_num = auto_num;
                                let doc_analysis_text = resp_cust_code;
                                let doc_tran_date = pago.fecha;
                                let doc_ref_text = resp_cust_code;
                                let doc_ref_num = (documentos[i].resp_inv_num==''||documentos[i].resp_inv_num==null||documentos[i].resp_inv_num==undefined)?0:documentos[i].resp_inv_num;
                                let doc_debit_amt = 0;
                                let doc_credit_amt = curr_pay;
                                let doc_debit_amt1 = 0;
                                let doc_credit_amt1 = curr_pay/Number(pago.conversion);
                                let doc_debit_amt2 = 0;
                                let doc_credit_amt2 = 0;
                                let doc_debit_amt3 = 0;
                                let doc_credit_amt3 = 0;
                                let doc_ref_text1 = pago.n_carpeta;
                                let doc_bran_code = '1';
                                let doc_profit_code = 'CM';
                                let doc_currency_code = 'USD';
                                let doc_rate_exchange = 1;
                                let doc_desc_text = razon_social.replace('&', '');

                                let doc_acct_code = '11301001-001';
                                let doc_ref_text2 = 'PAGO DIN WSC';

                                if (documentos[i].resp_doc_code != 'DI') {
                                    doc_acct_code = '11301001-000';
                                    doc_ref_text2 = 'PAGO FACT';
                                }

                                columna = '';                           valor = '';
                                columna+=`"fk_cabecera",`;              valor+=`'`+ doc_fk_cabecera +`',`;
                                columna+=`"seq_num",`;                  valor+=`'`+ doc_seq_num +`',`;
                                columna+=`"analysis_text",`;            valor+=`'`+ doc_analysis_text +`',`;
                                columna+=`"tran_date",`;                valor+=`'`+ doc_tran_date +`',`;
                                columna+=`"ref_text",`;                 valor+=`'`+ doc_ref_text +`',`;
                                columna+=`"ref_num",`;                  valor+=`'`+ doc_ref_num +`',`;
                                columna+=`"debit_amt",`;                valor+=`'`+ doc_debit_amt +`',`;
                                columna+=`"credit_amt",`;               valor+=`'`+ doc_credit_amt +`',`;
                                columna+=`"debit_amt1",`;               valor+=`'`+ doc_debit_amt1 +`',`;
                                columna+=`"credit_amt1",`;              valor+=`'`+ doc_credit_amt1 +`',`;
                                columna+=`"debit_amt2",`;               valor+=`'`+ doc_debit_amt2 +`',`;
                                columna+=`"credit_amt2",`;              valor+=`'`+ doc_credit_amt2 +`',`;
                                columna+=`"debit_amt3",`;               valor+=`'`+ doc_debit_amt3 +`',`;
                                columna+=`"credit_amt3",`;              valor+=`'`+ doc_credit_amt3 +`',`;
                                columna+=`"ref_text1",`;                valor+=`'`+ doc_ref_text1 +`',`;
                                columna+=`"bran_code",`;                valor+=`'`+ doc_bran_code +`',`;
                                columna+=`"profit_code",`;              valor+=`'`+ doc_profit_code +`',`;
                                columna+=`"currency_code",`;            valor+=`'`+ doc_currency_code +`',`;
                                columna+=`"rate_exchange",`;            valor+=`'`+ doc_rate_exchange +`',`;
                                columna+=`"acct_code",`;                valor+=`'`+ doc_acct_code +`',`;
                                columna+=`"ref_text2",`;                valor+=`'`+ doc_ref_text2 +`',`;
                                columna+=`"desc_text"`;                 valor+=`'`+ doc_desc_text +`'`;

                                save_detalle(columna, valor);
                                /** XML DETALLE DOCUMENTO **/
                                xmltext_aux += `<DETAIL>`;
                                xmltext_aux += `<CMPY_CODE>`+ '04' +`</CMPY_CODE>`;
                                xmltext_aux += `<SEQ_NUM>`+ doc_seq_num +`</SEQ_NUM>`;
                                xmltext_aux += `<TRAN_TYPE_IND>`+ 'CPA' +`</TRAN_TYPE_IND>`;
                                xmltext_aux += `<ANALYSIS_TEXT>`+ doc_analysis_text +`</ANALYSIS_TEXT>`;
                                xmltext_aux += `<TRAN_DATE>`+ doc_tran_date +`</TRAN_DATE>`;
                                xmltext_aux += `<REF_TEXT>`+ doc_ref_text +`</REF_TEXT>`;
                                xmltext_aux += `<REF_NUM>`+ doc_ref_num +`</REF_NUM>`;
                                xmltext_aux += `<ACCT_CODE>`+ doc_acct_code +`</ACCT_CODE>`;
                                xmltext_aux += `<DEBIT_AMT>`+ doc_debit_amt +`</DEBIT_AMT>`;
                                xmltext_aux += `<CREDIT_AMT>`+ doc_credit_amt +`</CREDIT_AMT>`;
                                xmltext_aux += `<DEBIT_AMT1>`+ doc_debit_amt1 +`</DEBIT_AMT1>`;
                                xmltext_aux += `<CREDIT_AMT1>`+ doc_credit_amt1 +`</CREDIT_AMT1>`;
                                xmltext_aux += `<DEBIT_AMT2>`+ doc_debit_amt2 +`</DEBIT_AMT2>`;
                                xmltext_aux += `<CREDIT_AMT2>`+ doc_credit_amt2 +`</CREDIT_AMT2>`;
                                xmltext_aux += `<REF_TEXT1>`+ doc_ref_text1 +`</REF_TEXT1>`;
                                xmltext_aux += `<REF_TEXT2>`+ doc_ref_text2 +`</REF_TEXT2>`;
                                xmltext_aux += `<DESC_TEXT>`+ doc_desc_text +`</DESC_TEXT>`;
                                xmltext_aux += `<BRAN_CODE>`+ doc_bran_code +`</BRAN_CODE>`;
                                xmltext_aux += `<PROFIT_CODE>`+ doc_profit_code +`</PROFIT_CODE>`;
                                xmltext_aux += `<CURRENCY_CODE>`+ doc_currency_code +`</CURRENCY_CODE>`;
                                xmltext_aux += `<RATE_EXCHANGE>`+ doc_rate_exchange +`</RATE_EXCHANGE>`;
                                xmltext_aux += `<DEBIT_AMT3>`+ doc_debit_amt3 +`</DEBIT_AMT3>`;
                                xmltext_aux += `<CREDIT_AMT3>`+ doc_credit_amt3 +`</CREDIT_AMT3>`;
                                xmltext_aux += `<REF_AMT>`+ doc_credit_amt +`</REF_AMT>`;
                                xmltext_aux += `<JOUR_CODE >`+ 'IPC' +`</JOUR_CODE >`;

                                xmltext_aux += `</DETAIL>`;

                                if (payment == 0) {
                                    break;
                                }
                                auto_num++;
                            }

                            if (payment > 0) {//SOBRO DINERO, SE AGREGA A AJUSTE

                                let aju_fk_cabecera = fk_cabecera;
                                let aju_seq_num = auto_num;
                                let aju_analysis_text = resp_cust_code;
                                let aju_tran_date = pago.fecha;
                                let aju_ref_text = resp_cust_code;
                                let aju_ref_num = 0;
                                let aju_debit_amt = 0;
                                let aju_credit_amt = payment;
                                let aju_debit_amt1 = 0;
                                let aju_credit_amt1 = payment/Number(pago.conversion);
                                let aju_debit_amt2 = 0;
                                let aju_credit_amt2 = 0;
                                let aju_debit_amt3 = 0;
                                let aju_credit_amt3 = 0;
                                let aju_ref_text1 = pago.n_carpeta;
                                let aju_bran_code = '';
                                let aju_profit_code = '';
                                let aju_currency_code = 'USD';
                                let aju_rate_exchange = 1;

                                let aju_acct_code = '21102001-000';
                                let aju_ref_text2 = 'EXCEDENTE CLIENTE';
                                let aju_desc_text = razon_social.replace('&', '');

                                columna = '';                           valor = '';
                                columna+=`"fk_cabecera",`;              valor+=`'`+ aju_fk_cabecera +`',`;
                                columna+=`"seq_num",`;                  valor+=`'`+ aju_seq_num +`',`;
                                columna+=`"analysis_text",`;            valor+=`'`+ aju_analysis_text +`',`;
                                columna+=`"tran_date",`;                valor+=`'`+ aju_tran_date +`',`;
                                columna+=`"ref_text",`;                 valor+=`'`+ aju_ref_text +`',`;
                                columna+=`"ref_num",`;                  valor+=`'`+ aju_ref_num +`',`;
                                columna+=`"debit_amt",`;                valor+=`'`+ aju_debit_amt +`',`;
                                columna+=`"credit_amt",`;               valor+=`'`+ aju_credit_amt +`',`;
                                columna+=`"debit_amt1",`;               valor+=`'`+ aju_debit_amt1 +`',`;
                                columna+=`"credit_amt1",`;              valor+=`'`+ aju_credit_amt1 +`',`;
                                columna+=`"debit_amt2",`;               valor+=`'`+ aju_debit_amt2 +`',`;
                                columna+=`"credit_amt2",`;              valor+=`'`+ aju_credit_amt2 +`',`;
                                columna+=`"debit_amt3",`;               valor+=`'`+ aju_debit_amt3 +`',`;
                                columna+=`"credit_amt3",`;              valor+=`'`+ aju_credit_amt3 +`',`;
                                columna+=`"ref_text1",`;                valor+=`'`+ aju_ref_text1 +`',`;
                                columna+=`"bran_code",`;                valor+=`'`+ aju_bran_code +`',`;
                                columna+=`"profit_code",`;              valor+=`'`+ aju_profit_code +`',`;
                                columna+=`"currency_code",`;            valor+=`'`+ aju_currency_code +`',`;
                                columna+=`"rate_exchange",`;            valor+=`'`+ aju_rate_exchange +`',`;
                                columna+=`"acct_code",`;                valor+=`'`+ aju_acct_code +`',`;
                                columna+=`"ref_text2",`;                valor+=`'`+ aju_ref_text2 +`',`;
                                columna+=`"desc_text"`;                 valor+=`'`+ aju_desc_text +`'`;

                                save_detalle(columna, valor);
                                /** XML DETALLE BANCO  **/
                                xmltext_aux += `<DETAIL>`;
                                xmltext_aux += `<CMPY_CODE>`+ '04' +`</CMPY_CODE>`;
                                xmltext_aux += `<SEQ_NUM>`+ aju_seq_num +`</SEQ_NUM>`;
                                xmltext_aux += `<TRAN_TYPE_IND>`+ 'AJU' +`</TRAN_TYPE_IND>`;
                                xmltext_aux += `<ANALYSIS_TEXT>`+ aju_analysis_text +`</ANALYSIS_TEXT>`;
                                xmltext_aux += `<TRAN_DATE>`+ aju_tran_date +`</TRAN_DATE>`;
                                xmltext_aux += `<REF_TEXT>`+ aju_ref_text +`</REF_TEXT>`;
                                xmltext_aux += `<REF_NUM>`+ aju_ref_num +`</REF_NUM>`;
                                xmltext_aux += `<ACCT_CODE>`+ aju_acct_code +`</ACCT_CODE>`;
                                xmltext_aux += `<DEBIT_AMT>`+ aju_debit_amt +`</DEBIT_AMT>`;
                                xmltext_aux += `<CREDIT_AMT>`+ aju_credit_amt +`</CREDIT_AMT>`;
                                xmltext_aux += `<DEBIT_AMT1>`+ aju_debit_amt1 +`</DEBIT_AMT1>`;
                                xmltext_aux += `<CREDIT_AMT1>`+ aju_credit_amt1 +`</CREDIT_AMT1>`;
                                xmltext_aux += `<DEBIT_AMT2>`+ aju_debit_amt2 +`</DEBIT_AMT2>`;
                                xmltext_aux += `<CREDIT_AMT2>`+ aju_credit_amt2 +`</CREDIT_AMT2>`;
                                xmltext_aux += `<REF_TEXT1>`+ aju_ref_text1 +`</REF_TEXT1>`;
                                xmltext_aux += `<REF_TEXT2>`+ aju_ref_text2 +`</REF_TEXT2>`;
                                xmltext_aux += `<DESC_TEXT>`+ aju_desc_text +`</DESC_TEXT>`;
                                xmltext_aux += `<BRAN_CODE>`+ aju_bran_code +`</BRAN_CODE>`;
                                xmltext_aux += `<PROFIT_CODE>`+ aju_profit_code +`</PROFIT_CODE>`;
                                xmltext_aux += `<CURRENCY_CODE>`+ aju_currency_code +`</CURRENCY_CODE>`;
                                xmltext_aux += `<RATE_EXCHANGE>`+ aju_rate_exchange +`</RATE_EXCHANGE>`;
                                xmltext_aux += `<DEBIT_AMT3>`+ aju_debit_amt3 +`</DEBIT_AMT3>`;
                                xmltext_aux += `<CREDIT_AMT3>`+ aju_credit_amt3 +`</CREDIT_AMT3>`;
                                xmltext_aux += `<REF_AMT>`+ aju_credit_amt +`</REF_AMT>`;
                                xmltext_aux += `<JOUR_CODE >`+ 'IPC' +`</JOUR_CODE >`;

                                xmltext_aux += `</DETAIL>`;
                            }

                            xmltext_aux +=`</BATCH></string>`;
                            send_pago_maximise(xmltext_aux, fk_cabecera)

                        }
                    }
                });
            } catch (error) {
                return false

            }
        }

        async function get_cliente_rut(fk_cliente) {

            var rut_cliente = await client.query(`SELECT rut FROM public.clientes WHERE id='${fk_cliente}'`);
            if (rut_cliente.rows.length > 0) {
                return rut_cliente.rows[0]['rut'].replace('.', '').replace('.', '');
            } else {
                return false
            }

        }

        async function save_detalle(columna, valor) {

            console.log(` INSERT INTO public.wsc_envio_asientos_detalles (`+columna+`) VALUES (`+valor+`) RETURNING *`);
            var insert = await client.query(` INSERT INTO public.wsc_envio_asientos_detalles (`+columna+`) VALUES (`+valor+`) RETURNING *`);

        }

        async function send_pago_maximise(xml, fk_cabecera) {

            console.log(xml);

            const request = require('request');
            const fs = require("fs");

            const data = {
                AliasName: 'dwi_tnm',
                UserName: 'webservice',
                Password: 'webservice001',
                Data: xml
            };

            const options = {
                url: 'http://asp3.maximise.cl/wsv/batch.asmx/SaveDocument',
                method: 'POST',
                headers: {
                    'Content-Type': 'text/html',
                    'Content-Length': data.length
                },
                json: true,
                form: {
                    AliasName: 'dwi_tnm',
                    UserName: 'webservice',
                    Password: 'webservice001',
                    Data: xml
                }
            };


            request.post(options, (err, res, body) => {

                if(err) {
                    console.log('error, ' + err);
                }
                else if(body) {
                    console.log(" RESPUESTA ");
                    if(res.statusCode!=200 ){

                        console.log("ERROR ");
                        console.log(JSON.stringify(res.body));

                    } else if(res.statusCode==200 ) {

                        console.log("\n\nSUCCESS ");
                        console.log("\n\n"+JSON.stringify(res.body));
                        var RespMax = JSON.stringify(res.body);
                        var IdMax = RespMax.match(/<int xmlns=\"Maximise\">([^<]*)<\/int>/);
                        console.log('\n\nId Maximise '+IdMax);

                        actualizar_estado_bd('MAXIMISE', fk_cabecera, IdMax)
                    }
                }
            });


        }

        async function actualizar_estado_bd(estado, fk_cabecera, IdMax) {

            var moment = require('moment');
            let fecha = moment(Date.now()).format("YYYY-MM-DD HH:mm:ss");

            var query = '';
                query+=`estado='`+estado+`', `;
                query+=`id_maximise='`+IdMax+`', `;
                query+=`"fk_updatedBy"=`+carpeta_data.rows[0]["fk_createdBy"]+`, `;
                query+=`"updatedAt"='`+fecha+`'`;

            console.log(`UPDATE public.wsc_envio_asientos_cabeceras SET `+query+` WHERE id=`+fk_cabecera+` RETURNING *`);
            await client.query(`UPDATE public.wsc_envio_asientos_cabeceras SET `+query+` WHERE id=`+fk_cabecera+` RETURNING *`);

            client.query(` UPDATE public.queue_maximise SET estado='${estado}' WHERE id=${tarea.id} `);

        }
    }
}

exports.orquestador_server_1 = async (req, resp) => {
    console.log("$.$");
    console.log("$.$");
    console.log("SCHEDULER MAXIMISE");
    var sche_envio_maximise = require('node-schedule');

    sche_envio_maximise.scheduleJob('*/30 * * * * *', () => {
        console.log('');
        console.log("CONSULTANDO COLA ");
        console.log("$$$");
        orquestador_server_1();

    });

    async function orquestador_server_1() {
        var tarea = await client.query(` 
        SELECT 
        * 
        FROM public.queue_maximise 
        WHERE 
        ( estado='PENDIENTE' and tarea='DOCUMENTOS COMER' )
        or (estado='PENDIENTE' and tarea='DOCUMENTOS' )
        ORDER BY id ASC limit 1
        `);
        console.log("$$$$$$");
        if(tarea.rows.length > 0) {
            console.log('TAREA ENCONTRADA: ENVIO DE ' + tarea.rows[0]['tarea']);

            if (tarea.rows[0]['tarea'] == 'DOCUMENTOS') {
                enviar_documento_maximise_wsc(tarea.rows[0])

            } else if (tarea.rows[0]['tarea'] == 'DOCUMENTOS COMER') {
                enviar_documento_maximise_comer(tarea.rows[0])

            }

        } else {
            console.log('NO HAY TAREAS PENDIENTES EN COLA');
        }
    }

    async function enviar_documento_maximise_wsc(tarea) {
        console.log('Envio de documento');
        client.query(` UPDATE public.queue_maximise SET estado='PROCESANDO' WHERE id=${tarea.id} `);

        var moment = require('moment');

        var today = new Date();
        var today_ano = today.getFullYear()*1;
        var today_mes = today.getMonth()+1; if(today_mes<10) { today_mes = '0'+today_mes; }
        var today_dia = today.getDate()*1; if(today_dia<10) { today_dia = '0'+today_dia; }
        var today_hora = (today.getHours()*1) - 4; if(today_hora<10) { today_hora = '0'+today_hora; }
        var today_minuto = today.getMinutes()*1; if(today_minuto<10) { today_minuto = '0'+today_minuto; }

        var dateTime = today_ano+'-'+today_mes+'-'+today_dia+' '+today_hora+':'+today_minuto;

        var facturas = await client.query(` SELECT * FROM public.wsc_envio_facturas_cabeceras2 where id=${tarea['fk_cabecera']}`);

        console.log('\n\nFACTURAS '+facturas.rows.length+'\n\n');
        if(facturas.rows.length>0)
        {

            let fk_nota_cobro = facturas.rows[0]['fk_nota_cobro'];

            var detalles = await client.query(`
            SELECT
            *
            FROM public.wsc_envio_facturas_detalles2
            where
            fk_cabecera=`+facturas.rows[0]['numero_unico']+`
            and anomesdiahora=`+facturas.rows[0]['anomesdiahora']+`
            `);
            console.log('\n\DETALLES '+detalles.rows.length+'\n\n');
            if(detalles.rows.length>0)
            {
                if (facturas.rows[0]['total_amt'] > 0)
                {
                    var inv_date_aux = facturas.rows[0]['inv_date'].split('-');
                    var contador = await client.query(`
                        SELECT count(*)+1 as cont
                        FROM wsc_envio_facturas_cabeceras2
                        WHERE doc_code='${facturas.rows[0]['doc_code']}' AND fk_nota_cobro=${fk_nota_cobro}
                    `);

                    let monto = facturas.rows[0]['total_amt'];
                    var flag_refactura =false;
                    var flag_pago_duplicado = false;


                    // PREGUNTO A MAXIMISE SI HAY ALGO QUE YA ESTE PAGADO
                    const request = require('request');
                    const fs = require("fs");

                    const xmltext = `
                                        SELECT ih.INV_NUM, ih.doc_code, ih.cust_code, ih.PAID_AMT, ih.TOTAL_AMT,ih.REF_TEXT1,ih.REF_TEXT2, ih.ext_num,
                                        (SELECT top 1 TOTAL_AMT FROM credithead ch WHERE ch.inv_num = ih.inv_num AND POSTED_FLAG<>'V' AND CMPY_CODE='04') as total_credito
                                        FROM invoicehead ih
                                        WHERE ih.cmpy_code='04' and
                                            ih.POSTED_FLAG<>'V' and
                                            ih.ref_text1='${facturas.rows[0]['ref_text1']}' and
                                            ((ih.doc_code='${facturas.rows[0]['doc_code']}' and
                                            (
                                                ((ih.com_text like 'EXENTO%' OR ih.com_text LIKE '%EXENTO%' or ih.com_text like 'AFECTO%' OR ih.com_text LIKE '%AFECTO%') and ih.com_text like '${facturas.rows[0]['com_text'].split(' ')[0]}%' OR ih.com_text LIKE '%${facturas.rows[0]['com_text'].split(' ')[0]}%')
                                                or
                                                not (ih.com_text like 'EXENTO%' OR ih.com_text LIKE '%EXENTO%' or ih.com_text like 'AFECTO%' OR ih.com_text LIKE '%AFECTO%')
                                            )
                                            ) or ih.doc_code='D1')
                                        ORDER BY ih.inv_num DESC;
                                    `;

                    console.log('xml', xmltext);

                    const data_get = {
                        AliasName: 'dwi_tnm',
                        UserName: 'webservice',
                        Password: 'webservice001',
                        Sql: xmltext
                    };

                    const options_get = {
                        url: 'http://asp3.maximise.cl/wsv/query.asmx/GetQueryAsDataSet',
                        method: 'POST',
                        headers: {
                            'Content-Type': 'text/html',
                            'Content-Length': data_get.length
                        },
                        json: true,
                        form: {
                            AliasName: 'dwi_tnm',
                            UserName: 'webservice',
                            Password: 'webservice001',
                            Sql: xmltext
                        }
                    };


                    request.post(options_get, (err, res, body) => {

                        if(err)
                        {
                            console.log(err);
                        }
                        else if(body)
                        {
                            console.log("\n\nRESPONSE GET INVOICEHEAD ");
                            console.log("\n\nRESPONSE STATUS CODE "+res.statusCode);
                            console.log("\n\nRESPONSE BODY "+JSON.stringify(res.body));
                            if(JSON.stringify(res.body).includes("System.Web.Services.Protocols.SoapException:")){

                                console.log("System.Web.Services.Protocols.SoapException:");
                                console.log(JSON.stringify(res.body));

                            }
                            else if(res.statusCode==200 )
                            {
                                var entries = JSON.stringify(body).split('<Table');

                                var vistos = {};

                                for(var i=1; i<entries.length; i++) {

                                    let inv_num = entries[i].split('<INV_NUM>').pop().split('</INV_NUM>')[0];
                                    let doc_code = entries[i].split('<doc_code>').pop().split('</doc_code>')[0];
                                    let cust_code = entries[i].split('<cust_code>').pop().split('</cust_code>')[0];
                                    let paid_amt = entries[i].split('<PAID_AMT>').pop().split('</PAID_AMT>')[0];
                                    let total_amt = entries[i].split('<TOTAL_AMT>').pop().split('</TOTAL_AMT>')[0];
                                    let ref_text1 = entries[i].split('<REF_TEXT1>').pop().split('</REF_TEXT1>')[0];
                                    let ref_text2 = entries[i].split('<REF_TEXT2>').pop().split('</REF_TEXT2>')[0];
                                    let ext_num = entries[i].split('<ext_num>').pop().split('</ext_num>')[0];
                                    let total_credito = entries[i].split('<total_credito>').pop().split('</total_credito>')[0];
                                    let cred_num = entries[i].split('<cred_num>').pop().split('</cred_num>')[0];

                                    if (doc_code == 'D1') {
                                        monto = monto + Number(total_amt);
                                    } else {
                                        monto = monto - Number(total_amt);
                                        try {
                                            if (Number(total_credito) > 0) {
                                                monto = monto + Number(total_credito);
                                            }
                                        } catch {

                                        }
                                    }

                                    flag_refactura = true;

                                }

                                if (monto > 0) {

                                    if (flag_refactura && facturas.rows[0]['doc_code'] == 'P8') {
                                        update_estado_factura('NO SE PUEDE REFACTURAR P8', facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);
                                    } else {
                                        enviar_documento_maximise(facturas, inv_date_aux, today_ano, today_mes, today_dia, monto, contador, i, detalles, flag_refactura);
                                    }
                                }
                            }
                        }
                    });



                    async function enviar_documento_maximise(facturas, inv_date_aux, today_ano, today_mes, today_dia, monto, contador, i, detalles, flag_refactura)
                    {
                        console.log('ENVIAR DOCUMENTO', monto)
                        var xmltext_aux = `<string xmlns="Maximise"><CUSTOMERINVOICE><HEAD>`;
                        xmltext_aux += `<CMPY_CODE>`+facturas.rows[0]['cmpy_code']+`</CMPY_CODE>`;
                        xmltext_aux += facturas.rows[0]['doc_code']=='P9'?`<TAX_CODE>IDF</TAX_CODE>`:`<TAX_CODE>EXE</TAX_CODE>`;
                        xmltext_aux += `<SALE_CODE>NA</SALE_CODE>`;
                        xmltext_aux += `<BRAN_CODE>1</BRAN_CODE>`;
                        xmltext_aux += `<CUST_CODE>`+facturas.rows[0]['cust_code']+`</CUST_CODE>`;
                        xmltext_aux += `<DOC_CODE>`+facturas.rows[0]['doc_code']+`</DOC_CODE>`;
                        xmltext_aux += `<INV_DATE>`+moment(inv_date_aux[2]+"-"+inv_date_aux[1]+"-"+inv_date_aux[0]+" 00:00:00").format('YYYY-MM-DD HH:mm')+`</INV_DATE>`;
                        xmltext_aux += `<ENTRY_CODE>webservice(PRUEBA)</ENTRY_CODE>`;
                        xmltext_aux += `<ENTRY_DATE>`+moment(today_ano+"-"+today_mes+"-"+today_dia+" 00:00:00").format('YYYY-MM-DD HH:mm')+`</ENTRY_DATE>`;
                        xmltext_aux += `<TERM_CODE>`+facturas.rows[0]['term_code']+`</TERM_CODE>`;
                        xmltext_aux += facturas.rows[0]['doc_code']=='P9'?`<TAX_PER>19</TAX_PER>`:`<TAX_PER>0</TAX_PER>`;
                        xmltext_aux += `<RATE_EXCHANGE>1</RATE_EXCHANGE>`;

                        if (flag_refactura) {

                            if (facturas.rows[0]['doc_code'] == 'DI') {
                                xmltext_aux += `<GOODS_AMT>`+monto+`</GOODS_AMT>`;
                                xmltext_aux += `<EXCENT_AMT>`+monto+`</EXCENT_AMT>`;
                                xmltext_aux += `<TOTAL_AMT>`+monto+`</TOTAL_AMT>`;
                            } else if (facturas.rows[0]['doc_code'] == 'PF') {
                                xmltext_aux += `<GOODS_AMT>`+monto+`</GOODS_AMT>`;
                                xmltext_aux += `<EXCENT_AMT>`+monto+`</EXCENT_AMT>`;
                                xmltext_aux += `<TOTAL_AMT>`+monto+`</TOTAL_AMT>`;
                            } else if (facturas.rows[0]['doc_code'] == 'P8') {
                                xmltext_aux += `<GOODS_AMT>`+monto+`</GOODS_AMT>`;
                                xmltext_aux += `<EXCENT_AMT>`+monto+`</EXCENT_AMT>`;
                                xmltext_aux += `<TOTAL_AMT>`+monto+`</TOTAL_AMT>`;
                                xmltext_aux += `<THRDUE_AMT>`+0+`</THRDUE_AMT>`;
                            } else if (facturas.rows[0]['doc_code'] == 'P9') {
                                xmltext_aux += `<GOODS_AMT>`+monto+`</GOODS_AMT>`;
                                xmltext_aux += `<EXCENT_AMT>`+0+`</EXCENT_AMT>`;
                                xmltext_aux += `<TOTAL_AMT>`+Number(Number(monto)*1.19).toFixed(0)+`</TOTAL_AMT>`;
                                xmltext_aux += `<THRDUE_AMT>`+0+`</THRDUE_AMT>`;
                            }

                            xmltext_aux += `<COST_AMT>0</COST_AMT>`;
                            xmltext_aux += `<PAID_AMT>0</PAID_AMT>`;

                            if(facturas.rows[0]['doc_code']=='P8'){

                                xmltext_aux += `<TAX_AMT>0</TAX_AMT>`;

                            }

                            var comuna = String(facturas.rows[0]['idcomuna']).length==5?String(facturas.rows[0]['idcomuna']):'0'+String(facturas.rows[0]['idcomuna']);
                            var ciudad = String(facturas.rows[0]['idciudad']);

                            xmltext_aux += `<DUE_DATE>`+facturas.rows[0]['due_date']+`</DUE_DATE>`;
                            xmltext_aux += `<POSTED_FLAG>N</POSTED_FLAG>`;
                            xmltext_aux += `<PRINTED_FLAG>N</PRINTED_FLAG>`;
                            xmltext_aux += `<STORY_FLAG>N</STORY_FLAG>`;
                            xmltext_aux += `<SHIP_CODE>`+facturas.rows[0]['cust_code']+`</SHIP_CODE>`;
                            xmltext_aux += `<NAME_TEXT>`+facturas.rows[0]['name_text']+`</NAME_TEXT>`;
                            xmltext_aux += `<ADDR_TEXT>`+facturas.rows[0]['addr_text']+`</ADDR_TEXT>`;
                            xmltext_aux += `<IDCOMUNA>`+comuna+`</IDCOMUNA>`;
                            xmltext_aux += `<IDCIUDAD>`+ciudad+`</IDCIUDAD>`;
                            xmltext_aux += `<IDDISTRITO>`+comuna+`</IDDISTRITO>`;
                            xmltext_aux += `<IDPAIS>`+facturas.rows[0]['idpais']+`</IDPAIS>`;
                            xmltext_aux += `<BILL_CODE>`+facturas.rows[0]['cust_code']+`</BILL_CODE>`;
                            xmltext_aux += `<BILL_NAME_TEXT>`+facturas.rows[0]['name_text']+`</BILL_NAME_TEXT>`;
                            xmltext_aux += `<BILL_ADDR_TEXT>`+facturas.rows[0]['addr_text']+`</BILL_ADDR_TEXT>`;
                            xmltext_aux += `<BILL_IDCOMUNA>`+comuna+`</BILL_IDCOMUNA>`;
                            xmltext_aux += `<BILL_IDCIUDAD>`+ciudad+`</BILL_IDCIUDAD>`;
                            xmltext_aux += `<BILL_IDDISTRITO>`+comuna+`</BILL_IDDISTRITO>`;
                            xmltext_aux += `<BILL_IDPAIS>`+facturas.rows[0]['idpais']+`</BILL_IDPAIS>`;
                            xmltext_aux += `<COM_TEXT>`+facturas.rows[0]['com_text']+`</COM_TEXT>`;
                            xmltext_aux += `<CURRENCY_CODE>`+facturas.rows[0]['currency_code']+`</CURRENCY_CODE>`;
                            xmltext_aux += `<AR_ACCT_CODE>`+facturas.rows[0]['ar_acct_code']+`</AR_ACCT_CODE>`;
                            xmltext_aux += `<YEAR_NUM>`+(Number(inv_date_aux[2])*1)+`</YEAR_NUM>`;
                            xmltext_aux += `<PERIOD_NUM>`+(Number(inv_date_aux[1])*1)+`</PERIOD_NUM>`;

                            if(facturas.rows[0]['doc_code']=='DI'){
                                xmltext_aux += `<EXT_NUM>`+facturas.rows[0]['ext_num']+`</EXT_NUM>`;
                            }else{
                                xmltext_aux += `<EXT_NUM>0</EXT_NUM>`;
                            }
                            xmltext_aux += `<COLECT_FLAG>S</COLECT_FLAG>`;
                            xmltext_aux += `<COST_POST_FLAG>N</COST_POST_FLAG>`;
                            xmltext_aux += `<FIRST_DUE_DATE>`+facturas.rows[0]['due_date']+`</FIRST_DUE_DATE>`;
                            xmltext_aux += `<PROFIT_CODE>`+facturas.rows[0]['profit_code']+`</PROFIT_CODE>`;
                            xmltext_aux += `<ANALYSIS_TEXT>`+facturas.rows[0]['analysis_text']+`</ANALYSIS_TEXT>`;
                            xmltext_aux += `<REF_TEXT1>`+facturas.rows[0]['ref_text1']+`</REF_TEXT1>`;
                            xmltext_aux += `<REF_TEXT2>`+facturas.rows[0]['ref_text2']+`</REF_TEXT2>`;
                            xmltext_aux += `<REFRATE_EXCHANGE>1</REFRATE_EXCHANGE>`;
                            xmltext_aux += `<RETTAX_AMT>0</RETTAX_AMT>`;
                            xmltext_aux += `<SELTAX_AMT>0</SELTAX_AMT>`;
                            xmltext_aux += `<DISC_IND>P</DISC_IND>`;
                            xmltext_aux += `<RET_AMT>0</RET_AMT>`;
                            xmltext_aux += `<RET_PER>0</RET_PER>`;
                            xmltext_aux += `</HEAD>`;

                            var NombreArchivo = facturas.rows[0]['ref_text1'];

                            xmltext_aux += `<DETAIL>`;
                            xmltext_aux += `<CMPY_CODE>`+facturas.rows[0]['cmpy_code']+`</CMPY_CODE>`;
                            xmltext_aux += `<LINE_NUM>`+(Number(i)+1)+`</LINE_NUM>`;
                            xmltext_aux += `<PART_CODE>`+detalles.rows[0]['part_code']+`</PART_CODE>`;
                            xmltext_aux += `<WARE_CODE>`+detalles.rows[0]['ware_code']+`</WARE_CODE>`;
                            xmltext_aux += `<UOM_CODE>`+detalles.rows[0]['uom_code']+`</UOM_CODE>`;
                            xmltext_aux += `<UOM_QTY>`+detalles.rows[0]['uom_qty']+`</UOM_QTY>`;
                            xmltext_aux += `<ORD_QTY>`+detalles.rows[0]['qrd_qty']+`</ORD_QTY>`;
                            xmltext_aux += `<LINE_TEXT>`+detalles.rows[0]['line_text']+`</LINE_TEXT>`;
                            xmltext_aux += `<UNIT_COST_AMT>`+detalles.rows[0]['unit_cost_amt']+`</UNIT_COST_AMT>`;
                            xmltext_aux += `<EXT_COST_AMT>`+detalles.rows[0]['ext_cost_amot']+`</EXT_COST_AMT>`;
                            xmltext_aux += `<DISC_PER>`+detalles.rows[0]['disc_per']+`</DISC_PER>`;
                            xmltext_aux += facturas.rows[0]['doc_code']!='DI'? `<UNIT_SALE_AMT>`+monto+`</UNIT_SALE_AMT>` : `<UNIT_SALE_AMT>`+detalles.rows[0]['unit_sale_amt']+`</UNIT_SALE_AMT>`;
                            xmltext_aux += facturas.rows[0]['doc_code']!='DI'? `<EXT_SALE_AMT>`+monto+`</EXT_SALE_AMT>` : `<EXT_SALE_AMT>`+detalles.rows[0]['ext_sale_amt']+`</EXT_SALE_AMT>`;
                            xmltext_aux += `<UNIT_EXCENT_AMT>`+monto+`</UNIT_EXCENT_AMT>`;
                            xmltext_aux += `<EXT_EXCENT_AMT>`+monto+`</EXT_EXCENT_AMT>`;
                            xmltext_aux += `<UNIT_TAX_AMT>`+detalles.rows[0]['unit_taxt_amt']+`</UNIT_TAX_AMT>`;
                            xmltext_aux += `<EXT_TAX_AMT>`+detalles.rows[0]['ext_tax_amt']+`</EXT_TAX_AMT>`;
                            xmltext_aux += `<LINE_TOTAL_AMT>`+monto+`</LINE_TOTAL_AMT>`;
                            xmltext_aux += `<LIST_PRICE_AMT>`+detalles.rows[0]['list_price_amt']+`</LIST_PRICE_AMT>`;
                            xmltext_aux += `<CUST_PRICE_AMT>`+monto+`</CUST_PRICE_AMT>`;
                            xmltext_aux += `<LINE_ACCT_CODE>`+detalles.rows[0]['line_acct_code']+`</LINE_ACCT_CODE>`;
                            xmltext_aux += `<LEVEL_CODE>`+detalles.rows[0]['level_code']+`</LEVEL_CODE>`;
                            xmltext_aux += `<BACK_QTY>`+detalles.rows[0]['back_qty']+`</BACK_QTY>`;
                            xmltext_aux += `<PENDING_FLAG>`+detalles.rows[0]['pending_flag']+`</PENDING_FLAG>`;
                            xmltext_aux += `<UNIT_COST_AMT1>`+detalles.rows[0]['unit_cost_amt1']+`</UNIT_COST_AMT1>`;
                            xmltext_aux += `<SHIP_DATE>`+detalles.rows[0]['ship_date']+`</SHIP_DATE>`;
                            xmltext_aux += `<PROFIT_CODE>`+detalles.rows[0]['profit_code']+`</PROFIT_CODE>`;
                            xmltext_aux += `<BRAN_CODE>`+detalles.rows[0]['brand_code']+`</BRAN_CODE>`;
                            xmltext_aux += `<UNIT_COMCOST_AMT>`+detalles.rows[0]['unit_comcost_amt']+`</UNIT_COMCOST_AMT>`;
                            xmltext_aux += `<UNIT_IFRSCOST_AMT>`+detalles.rows[0]['unit_ifrscost_amt']+`</UNIT_IFRSCOST_AMT>`;

                            if(facturas.rows[0]['doc_code']=='PF'){
                                xmltext_aux += `<REF_TEXT2>`+detalles.rows[0]['ref_text2']+`</REF_TEXT2>`;
                            }


                            if(facturas.rows[0]['doc_code']=='DI' || facturas.rows[0]['doc_code']=='PF'){
                                xmltext_aux += `<LINE_TEXT1>`+detalles.rows[0]['line_text1']+`</LINE_TEXT1>`;
                                xmltext_aux += `<REF_TEXT1>`+facturas.rows[0]['ref_text1']+`</REF_TEXT1>`;
                            }

                            xmltext_aux += `<TAXEDUNIT_PRICE_AMT>`+detalles.rows[0]['taxedunit_price_amt']+`</TAXEDUNIT_PRICE_AMT>`;
                            xmltext_aux += `</DETAIL>`;

                        } else {

                            xmltext_aux += `<GOODS_AMT>`+facturas.rows[0]['goods_amt']+`</GOODS_AMT>`;
                            xmltext_aux += `<EXCENT_AMT>`+facturas.rows[0]['excent_amt']+`</EXCENT_AMT>`;
                            xmltext_aux += `<TOTAL_AMT>`+facturas.rows[0]['total_amt']+`</TOTAL_AMT>`;
                            xmltext_aux += `<COST_AMT>0</COST_AMT>`;
                            xmltext_aux += `<PAID_AMT>0</PAID_AMT>`;

                            if(facturas.rows[0]['doc_code']=='P8'){

                                xmltext_aux += `<THRDUE_AMT>`+facturas.rows[0]['thrdue_amt']+`</THRDUE_AMT>`;
                                xmltext_aux += `<TAX_AMT>0</TAX_AMT>`;

                            }else if(facturas.rows[0]['doc_code']=='F9' || facturas.rows[0]['doc_code']=='P9'){
                                xmltext_aux += `<TAX_AMT>`+facturas.rows[0]['tax_amt']+`</TAX_AMT>`;
                                xmltext_aux += `<THRDUE_AMT>0</THRDUE_AMT>`;
                            }

                            var comuna = String(facturas.rows[0]['idcomuna']).length==5?String(facturas.rows[0]['idcomuna']):'0'+String(facturas.rows[0]['idcomuna']);
                            var ciudad = String(facturas.rows[0]['idciudad']);

                            xmltext_aux += `<DUE_DATE>`+facturas.rows[0]['due_date']+`</DUE_DATE>`;
                            xmltext_aux += `<POSTED_FLAG>N</POSTED_FLAG>`;
                            xmltext_aux += `<PRINTED_FLAG>N</PRINTED_FLAG>`;
                            xmltext_aux += `<STORY_FLAG>N</STORY_FLAG>`;
                            xmltext_aux += `<SHIP_CODE>`+facturas.rows[0]['cust_code']+`</SHIP_CODE>`;
                            xmltext_aux += `<NAME_TEXT>`+facturas.rows[0]['name_text']+`</NAME_TEXT>`;
                            xmltext_aux += `<ADDR_TEXT>`+facturas.rows[0]['addr_text']+`</ADDR_TEXT>`;
                            xmltext_aux += `<IDCOMUNA>`+comuna+`</IDCOMUNA>`;
                            xmltext_aux += `<IDCIUDAD>`+ciudad+`</IDCIUDAD>`;
                            xmltext_aux += `<IDDISTRITO>`+comuna+`</IDDISTRITO>`;
                            xmltext_aux += `<IDPAIS>`+facturas.rows[0]['idpais']+`</IDPAIS>`;
                            xmltext_aux += `<BILL_CODE>`+facturas.rows[0]['cust_code']+`</BILL_CODE>`;
                            xmltext_aux += `<BILL_NAME_TEXT>`+facturas.rows[0]['name_text']+`</BILL_NAME_TEXT>`;
                            xmltext_aux += `<BILL_ADDR_TEXT>`+facturas.rows[0]['addr_text']+`</BILL_ADDR_TEXT>`;
                            xmltext_aux += `<BILL_IDCOMUNA>`+comuna+`</BILL_IDCOMUNA>`;
                            xmltext_aux += `<BILL_IDCIUDAD>`+ciudad+`</BILL_IDCIUDAD>`;
                            xmltext_aux += `<BILL_IDDISTRITO>`+comuna+`</BILL_IDDISTRITO>`;
                            xmltext_aux += `<BILL_IDPAIS>`+facturas.rows[0]['idpais']+`</BILL_IDPAIS>`;
                            xmltext_aux += `<COM_TEXT>`+facturas.rows[0]['com_text']+`</COM_TEXT>`;
                            xmltext_aux += `<CURRENCY_CODE>`+facturas.rows[0]['currency_code']+`</CURRENCY_CODE>`;
                            xmltext_aux += `<AR_ACCT_CODE>`+facturas.rows[0]['ar_acct_code']+`</AR_ACCT_CODE>`;
                            xmltext_aux += `<YEAR_NUM>`+(Number(inv_date_aux[2])*1)+`</YEAR_NUM>`;
                            xmltext_aux += `<PERIOD_NUM>`+(Number(inv_date_aux[1])*1)+`</PERIOD_NUM>`;

                            if(facturas.rows[0]['doc_code']=='DI'){
                                xmltext_aux += `<EXT_NUM>`+facturas.rows[0]['ext_num']+`</EXT_NUM>`;
                            }else{
                                xmltext_aux += `<EXT_NUM>0</EXT_NUM>`;
                            }
                            xmltext_aux += `<COLECT_FLAG>S</COLECT_FLAG>`;
                            xmltext_aux += `<COST_POST_FLAG>N</COST_POST_FLAG>`;
                            xmltext_aux += `<FIRST_DUE_DATE>`+facturas.rows[0]['due_date']+`</FIRST_DUE_DATE>`;
                            xmltext_aux += `<PROFIT_CODE>`+facturas.rows[0]['profit_code']+`</PROFIT_CODE>`;
                            xmltext_aux += `<ANALYSIS_TEXT>`+facturas.rows[0]['analysis_text']+`</ANALYSIS_TEXT>`;
                            xmltext_aux += `<REF_TEXT1>`+facturas.rows[0]['ref_text1']+`</REF_TEXT1>`;
                            xmltext_aux += `<REF_TEXT2>`+facturas.rows[0]['ref_text2']+`</REF_TEXT2>`;
                            xmltext_aux += `<REFRATE_EXCHANGE>1</REFRATE_EXCHANGE>`;
                            xmltext_aux += `<RETTAX_AMT>0</RETTAX_AMT>`;
                            xmltext_aux += `<SELTAX_AMT>0</SELTAX_AMT>`;
                            xmltext_aux += `<DISC_IND>P</DISC_IND>`;
                            xmltext_aux += `<RET_AMT>0</RET_AMT>`;
                            xmltext_aux += `<RET_PER>0</RET_PER>`;
                            xmltext_aux += `</HEAD>`;

                            var NombreArchivo = facturas.rows[0]['ref_text1'];
                            for(var i=0; i<detalles.rows.length; i++)
                            {
                                xmltext_aux += `<DETAIL>`;
                                xmltext_aux += `<CMPY_CODE>`+facturas.rows[0]['cmpy_code']+`</CMPY_CODE>`;
                                xmltext_aux += `<LINE_NUM>`+(Number(i)+1)+`</LINE_NUM>`;
                                xmltext_aux += `<PART_CODE>`+detalles.rows[i]['part_code']+`</PART_CODE>`;
                                xmltext_aux += `<WARE_CODE>`+detalles.rows[i]['ware_code']+`</WARE_CODE>`;
                                xmltext_aux += `<UOM_CODE>`+detalles.rows[i]['uom_code']+`</UOM_CODE>`;
                                xmltext_aux += `<UOM_QTY>`+detalles.rows[i]['uom_qty']+`</UOM_QTY>`;
                                xmltext_aux += `<ORD_QTY>`+detalles.rows[i]['qrd_qty']+`</ORD_QTY>`;
                                xmltext_aux += `<LINE_TEXT>`+detalles.rows[i]['line_text']+`</LINE_TEXT>`;
                                xmltext_aux += `<UNIT_COST_AMT>`+detalles.rows[i]['unit_cost_amt']+`</UNIT_COST_AMT>`;
                                xmltext_aux += `<EXT_COST_AMT>`+detalles.rows[i]['ext_cost_amot']+`</EXT_COST_AMT>`;
                                xmltext_aux += `<DISC_PER>`+detalles.rows[i]['disc_per']+`</DISC_PER>`;
                                xmltext_aux += `<UNIT_SALE_AMT>`+detalles.rows[i]['unit_sale_amt']+`</UNIT_SALE_AMT>`;
                                xmltext_aux += `<EXT_SALE_AMT>`+detalles.rows[i]['ext_sale_amt']+`</EXT_SALE_AMT>`;
                                xmltext_aux += `<UNIT_EXCENT_AMT>`+detalles.rows[i]['unit_excent_amt']+`</UNIT_EXCENT_AMT>`;
                                xmltext_aux += `<EXT_EXCENT_AMT>`+detalles.rows[i]['ext_excent_amt']+`</EXT_EXCENT_AMT>`;
                                xmltext_aux += `<UNIT_TAX_AMT>`+detalles.rows[i]['unit_taxt_amt']+`</UNIT_TAX_AMT>`;
                                xmltext_aux += `<EXT_TAX_AMT>`+detalles.rows[i]['ext_tax_amt']+`</EXT_TAX_AMT>`;
                                xmltext_aux += `<LINE_TOTAL_AMT>`+detalles.rows[i]['line_total_amt']+`</LINE_TOTAL_AMT>`;
                                xmltext_aux += `<LIST_PRICE_AMT>`+detalles.rows[i]['list_price_amt']+`</LIST_PRICE_AMT>`;
                                xmltext_aux += `<CUST_PRICE_AMT>`+detalles.rows[i]['cust_price_amt']+`</CUST_PRICE_AMT>`;
                                xmltext_aux += `<LINE_ACCT_CODE>`+detalles.rows[i]['line_acct_code']+`</LINE_ACCT_CODE>`;
                                xmltext_aux += `<LEVEL_CODE>`+detalles.rows[i]['level_code']+`</LEVEL_CODE>`;
                                xmltext_aux += `<BACK_QTY>`+detalles.rows[i]['back_qty']+`</BACK_QTY>`;
                                xmltext_aux += `<PENDING_FLAG>`+detalles.rows[i]['pending_flag']+`</PENDING_FLAG>`;
                                xmltext_aux += `<UNIT_COST_AMT1>`+detalles.rows[i]['unit_cost_amt1']+`</UNIT_COST_AMT1>`;
                                xmltext_aux += `<SHIP_DATE>`+detalles.rows[i]['ship_date']+`</SHIP_DATE>`;
                                xmltext_aux += `<PROFIT_CODE>`+detalles.rows[i]['profit_code']+`</PROFIT_CODE>`;
                                xmltext_aux += `<BRAN_CODE>`+detalles.rows[i]['brand_code']+`</BRAN_CODE>`;
                                xmltext_aux += `<UNIT_COMCOST_AMT>`+detalles.rows[i]['unit_comcost_amt']+`</UNIT_COMCOST_AMT>`;
                                xmltext_aux += `<UNIT_IFRSCOST_AMT>`+detalles.rows[i]['unit_ifrscost_amt']+`</UNIT_IFRSCOST_AMT>`;

                                if(facturas.rows[0]['doc_code']=='PF'){
                                    xmltext_aux += `<REF_TEXT2>`+detalles.rows[i]['ref_text2']+`</REF_TEXT2>`;
                                }


                                if(facturas.rows[0]['doc_code']=='DI' || facturas.rows[0]['doc_code']=='PF'){
                                    xmltext_aux += `<LINE_TEXT1>`+detalles.rows[i]['line_text1']+`</LINE_TEXT1>`;
                                    xmltext_aux += `<REF_TEXT1>`+facturas.rows[0]['ref_text1']+`</REF_TEXT1>`;
                                }

                                xmltext_aux += `<TAXEDUNIT_PRICE_AMT>`+detalles.rows[i]['taxedunit_price_amt']+`</TAXEDUNIT_PRICE_AMT>`;
                                xmltext_aux += `</DETAIL>`;
                            }
                        }


                        xmltext_aux +=`</CUSTOMERINVOICE></string>`;

                        update_estado_factura('CARGADA MAXIMISE', facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);


                        const data = {
                            AliasName: 'dwi_tnm',
                            UserName: 'webservice',
                            Password: 'webservice001',
                            Data: xmltext_aux
                        };

                        const options = {
                            url: 'http://asp3.maximise.cl/wsv/Invoice.asmx/SaveDocument',
                            method: 'POST',
                            headers: {
                                'Content-Type': 'text/html',
                                'Content-Length': data.length
                            },
                            json: true,
                            form: {
                                AliasName: 'dwi_tnm',
                                UserName: 'webservice',
                                Password: 'webservice001',
                                Data: xmltext_aux
                            }
                        };
                        request.post(options, (err, res, body) => {

                            var texto_log = '';
                            if(err)
                            {
                                update_error_cargar_maximise(err, facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);
                            }
                            else if(body)
                            {
                                console.log(" --------------------------- RES "+facturas.rows[0]['ref_text1'] + ' -----------------------------');
                                console.log("  ");
                                if(JSON.stringify(res.body).includes("System.Web.Services.Protocols.SoapException:")){

                                    console.log(" --------------------------- ERROR ");
                                    update_error_cargar_maximise(JSON.stringify(res.body), facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);

                                }
                                else if(res.statusCode==200 )
                                {
                                    var numero_interno = JSON.stringify(body);
                                    numero_interno = numero_interno.replace('</int>', '');
                                    var id_maximise = numero_interno.substring(71, (Number(numero_interno.length)-1) );
                                    console.log(" --------------------------- BODY REPORT ");
                                    console.log(" --------------------------- "+id_maximise);
                                    console.log("  ");

                                    let numero, msg;
                                    let flag = false;

                                    if (facturas.rows[0]['doc_code'] == 'DI') {
                                        numero = 12;
                                        msg = 'Pago del IVA disponible';
                                        flag = true;
                                    } else if (facturas.rows[0]['doc_code'] == 'PF') {
                                        numero = 11;
                                        msg = 'Pago del servicio disponible';
                                        flag = true;
                                    }

                                    if (flag) {
                                        //enviar_notificacion(numero, msg,facturas.rows[0]['fk_nota_cobro']);
                                    }

                                    update_cargado_maximise(id_maximise, facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);
                                    if( facturas.rows[0]['servicio']=='SI' ){
                                        /* lalo */
                                        //enviar_documento_sii(id_maximise, facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], NombreArchivo, facturas.rows[0]['cmpy_code'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);
                                    }
                                }
                            }
                        });


                    }

                    async function enviar_documento_sii(id_maximise, cabecera, anomesdiahora, NombreArchivo, cmpy_code, fk_nota_cobro, doc_code)
                    {
                        update_estado_factura('ENVIANDO AL SII', cabecera, anomesdiahora, fk_nota_cobro, doc_code);

                        const data = {
                            AliasName: 'dwi_tnm',
                            UserName: 'webservice',
                            Password: 'webservice001',
                            DocumentNumber: id_maximise,
                            Company: cmpy_code
                        };

                        const options = {
                            url: 'http://asp3.maximise.cl/wsv/Invoice.asmx/PrintDocument',
                            method: 'POST',
                            headers: {
                                'Content-Type': 'text/html',
                                'Content-Length': data.length
                            },
                            json: true,
                            form: {
                                AliasName: 'dwi_tnm',
                                UserName: 'webservice',
                                Password: 'webservice001',
                                DocumentNumber: id_maximise,
                                Company: cmpy_code
                            }
                        };

                        request.post(options, (err, res, body) => {

                            var texto_log = '';
                            if(err)
                            {
                                update_error_cargar_sii('ERROR CARGAR SII', cabecera, anomesdiahora, fk_nota_cobro, doc_code);
                            }
                            else if(body)
                            {
                                console.log(" --------------------------- SII --------------------------");
                                console.log(JSON.stringify(body));
                                if(res.statusCode==200 )
                                {
                                    console.log(" --------------------------- BODY REPORT "+JSON.stringify(body));
                                    console.log("  ");

                                    var folio_sii = JSON.stringify(body);
                                    folio_sii = folio_sii.replace('</string>"', '');
                                    folio_sii = folio_sii.substring(74, (Number(folio_sii.length)) );

                                    actualizar_sii_factura (folio_sii, cabecera, anomesdiahora, fk_nota_cobro, doc_code);

                                    const data = {
                                        AliasName: 'dwi_tnm',
                                        UserName: 'webservice',
                                        Password: 'webservice001',
                                        DocumentNumber: id_maximise,
                                        Company: cmpy_code
                                    };


                                    const options = {
                                        url: 'http://asp3.maximise.cl/wsv/Invoice.asmx/DownloadPdf',
                                        method: 'POST',
                                        headers: {
                                            'Content-Type': 'text/html',
                                            'Content-Length': data.length
                                        },
                                        json: true,
                                        form: {
                                            AliasName: 'dwi_tnm',
                                            UserName: 'webservice',
                                            Password: 'webservice001',
                                            DocumentNumber: id_maximise,
                                            Company: cmpy_code
                                        }
                                    };

                                    const response_req = request.post(options, (err, res, body) => {

                                    });

                                    response_req.on('response', function (res) {

                                        res.pipe(fs.createWriteStream('C:/Users/Administrator/Documents/wscargo/restserver/public/files/fact_cargar_por_contenedor/'+NombreArchivo+'.pdf'));

                                    });

                                    //enviar_notificacion(13, 'Factura diponible del servicio disponible para entrega',fk_nota_cobro);
                                    enviar_documentacion_factura(fk_nota_cobro, NombreArchivo);

                                    return "OK";

                                }
                                else
                                {
                                    return "ERROR2";
                                }
                            }

                        });
                    }

                    async function update_cargado_maximise(id_maximise, numero_unico, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log (`CARGADA MAXIMISE `);

                        await client.query(`
                        UPDATE public.wsc_envio_facturas_cabeceras2
                        SET estado='CARGADA MAXIMISE'
                        , max_id_interno=`+id_maximise+`
                        where
                        numero_unico=`+numero_unico+`
                        and
                        anomesdiahora=`+anomesdiahora+`
                        `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(`
                        UPDATE public.notas_cobros_estados
                        SET estado='CARGADA MAXIMISE'
                        WHERE fk_provision=`+fk_nota_cobro+` AND fk_tipo=${codigo}`);


                        client.query(` UPDATE public.queue_maximise SET estado='CARGADA MAXIMISE' WHERE id=${tarea.id} `);

                    }

                    async function update_error_cargar_sii(err, numero_unico, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log(` ERROR CARGAR SII`);

                        await client.query(`
                        UPDATE public.wsc_envio_facturas_cabeceras2
                        SET
                        estado='ERROR CARGAR SII'
                        , log = concat(log,'\n','`+err+`')
                        where
                        numero_unico=`+numero_unico+`
                        and
                        anomesdiahora=`+anomesdiahora+`
                        `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(`
                        UPDATE public.notas_cobros_estados
                        SET estado='ERROR CARGAR SII'
                        WHERE fk_provision=`+fk_nota_cobro+`  AND fk_tipo=${codigo}`);

                        client.query(` UPDATE public.queue_maximise SET estado='ERROR CARGAR SII' WHERE id=${tarea.id} `);

                    }

                    async function update_error_cargar_maximise(err, numero_unico, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log(`ERROR CARGAR MAXIMISE`);
                        console.log(`
                        UPDATE public.wsc_envio_facturas_cabeceras2
                        SET
                        estado='ERROR CARGAR MAXIMISE'
                        , log = concat(log,'\n','`+err+`')
                        where
                        numero_unico=`+numero_unico+`
                        and
                        anomesdiahora=`+anomesdiahora+`
                        `);

                        await client.query(`
                        UPDATE public.wsc_envio_facturas_cabeceras2
                        SET
                        estado='ERROR CARGAR MAXIMISE'
                        , log = concat(log,'\n','`+err+`')
                        where
                        numero_unico=`+numero_unico+`
                        and
                        anomesdiahora=`+anomesdiahora+`
                        `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(`
                        UPDATE public.notas_cobros_estados
                        SET estado='ERROR CARGAR MAXIMISE'
                        WHERE fk_provision=`+fk_nota_cobro+` AND fk_tipo=${codigo}`);

                        client.query(` UPDATE public.queue_maximise SET estado='ERROR CARGAR MAXIMISE', info_extra='ERROR: ${err}' WHERE id=${tarea.id} `);

                    }

                    async function update_estado_factura(estado, cabecera, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log(estado);
                        await client.query(` UPDATE public.wsc_envio_facturas_cabeceras2 SET estado='`+estado+`' where numero_unico=`+cabecera+` and anomesdiahora=`+anomesdiahora+` `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(` UPDATE public.notas_cobros_estados SET estado='${estado}' WHERE fk_provision=`+fk_nota_cobro+` AND fk_tipo=${codigo} `);

                        client.query(` UPDATE public.queue_maximise SET estado='${estado}' WHERE id=${tarea.id} `);

                    }

                    async function actualizar_sii_factura(folio_sii, cabecera, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log(`ENVIADA SII`);
                        await client.query(` UPDATE public.wsc_envio_facturas_cabeceras2 SET estado='ENVIADA SII', sii_factura='`+folio_sii+`' where numero_unico=`+cabecera+` and anomesdiahora=`+anomesdiahora+` `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(` UPDATE public.notas_cobros_estados SET estado='ENVIADA SII' WHERE fk_provision=`+fk_nota_cobro+` AND fk_tipo=${codigo}`);

                        client.query(` UPDATE public.queue_maximise SET estado='ENVIADA SII' WHERE id=${tarea.id} `);

                    }

                    async function enviar_notificacion(nro_notificacion, message,fk_nota_cobro)
                    {

                        if(process.env.notificacionesClienteDigital){
                            let proforma=await client.query(`
                            SELECT
                                d.id,d.contenedor,d.fk_cliente,c.id as fk_contenedor,cp.id as fk_proforma,d.n_carpeta from
                                public.despachos d
                                INNER JOIN public.notas_cobros nc on nc.fk_despacho=d.id
                                INNER JOIN public.contenedor c on c.codigo=d.contenedor
                                INNER JOIN public.contenedor_proforma cp on cp.fk_contenedor=c.id
                                where nc.id=`+fk_nota_cobro+` and cp.estado>=0`);

                            if(proforma && proforma.rows && proforma.rows.length>0){
                                let resultP=await client.query(`
                                SELECT
                                t.fk_propuesta,t.fk_cliente,gc.fk_servicio,gc.referencia,ct.fk_consolidado,
                                upper(c."dteEmail") as email
                                FROM
                                public.contenedor_proforma_detalle cpd
                                INNER JOIN public.tracking_detalle td on td.id=cpd.fk_tracking_detalle
                                INNER JOIN public.tracking t on t.id=td.tracking_id
                                INNER JOIN public.consolidado_tracking ct on ct.fk_tracking=t.id
                                INNER JOIN public.gc_propuestas_cabeceras gc on gc.id=t.fk_propuesta
                                INNER JOIN public.clientes c on c.id=t.fk_cliente
                                WHERE cpd.fk_contenedor_proforma=`+proforma.rows[0].fk_proforma+` and t.fk_cliente=`+proforma.rows[0].fk_cliente+` group by t.fk_propuesta,t.fk_cliente,gc.fk_servicio,c."dteEmail",gc.referencia,ct.fk_consolidado`);

                            if(resultP && resultP.rows && resultP.rows.length>0){
                                for(let i=0;i<resultP.rows.length;i++){
                                    if(resultP.rows[i].fk_servicio!=null && resultP.rows[i].fk_servicio>0){
                                        let NotifConfig1=await Notifications.verifyConfigNotificationExpDigital(resultP.rows[i].fk_cliente, nro_notificacion/*NUMERO DE NOTIFICACION*/,
                                            async function(res2){
                                                if(res2!=null && res2.data){
                                                    if(res2.data.estado_web===true){
                                                        let insertNotification1=await Notifications.insertNotificationExpDigital({
                                                            fk_cliente:resultP.rows[i].fk_cliente,
                                                            texto:message,
                                                            fk_notificacion_configuracion:res2.data.id,
                                                            visto:false,
                                                            fk_servicio:resultP.rows[i].fk_servicio
                                                        },async function(res3){
                                                            console.log('res3',res3);
                                                        });
                                                    }

                                                    if(res2.data.estado_correo===true){
                                                        if(resultP.rows[i].email && resultP.rows[i].email.length>0){

                                                            let comercial=null;
                                                            let comer = await funcionesCompartidasCtrl.get_comercial_vigente(resultP.rows[i].fk_cliente);
                                                            if(comer && comer.rows && comer.rows.length>0){
                                                                comercial=comer.rows[0];
                                                            }

                                                            /*
                                                            let comercial=null;
                                                            if(res2.data.fk_comercial && res2.data.fk_comercial!=null){
                                                                let comer=await client.query(`SELECT concat(nombre,' ',apellidos) as nombre,email,telefono FROM public.usuario where id=`+res2.data.fk_comercial);
                                                                if(comer && comer.rows && comer.rows.length>0){
                                                                    comercial=comer.rows[0];
                                                                }
                                                            }
                                                            */

                                                            let asunto='';
                                                            let texto1='';
                                                            let texto=['Hola '+res2.data.razon_social.toUpperCase()+'.'];let tipoAtt=null;
                                                            if(nro_notificacion==11){//pago disponible
                                                                tipoAtt={tipo:nro_notificacion,carpeta:proforma.rows[0].n_carpeta};
                                                                asunto='WS Cargo | Pago disponible del servicio N° '+resultP.rows[i].fk_consolidado;
                                                                texto1='El costo del servicio N° '+resultP.rows[i].fk_consolidado;

                                                                if(resultP.rows[i].referencia!=null && resultP.rows[i].referencia.length>0){
                                                                    asunto+=' | '+resultP.rows[i].referencia;
                                                                    texto1+=' | '+resultP.rows[i].referencia+' ya se encuentra disponible para pago. En el documento adjunto encontrarás las instrucciones para realizar el pago. Si tienes dudas para realizar este proceso por favor contáctame. Recuerda que este pago es un requisito para poder despachar o retirar tu carga.';
                                                                }else{
                                                                    texto1+=' ya se encuentra disponible para pago. En el documento adjunto encontrarás las instrucciones para realizar el pago. Si tienes dudas para realizar este proceso por favor contáctame. Recuerda que este pago es un requisito para poder despachar o retirar tu carga.';
                                                                }

                                                                texto.push(texto1);texto.push('En el archivo adjunto puedes encontrar la factura correspondiente.');
                                                            }else if(nro_notificacion==12){//pago iva disponible
                                                                tipoAtt={tipo:nro_notificacion,carpeta:proforma.rows[0].n_carpeta};
                                                                asunto='WS Cargo | Pago disponible del IVA del servicio N° '+resultP.rows[i].fk_consolidado;
                                                                texto1='El costo del IVA de tu servicio N° '+resultP.rows[i].fk_consolidado;

                                                                if(resultP.rows[i].referencia!=null && resultP.rows[i].referencia.length>0){
                                                                    asunto+=' | '+resultP.rows[i].referencia;
                                                                    texto1+=' | '+resultP.rows[i].referencia+' ya se encuentra disponible para pago. En el documento adjunto encontrarás las instrucciones para realizar el pago. Si tienes dudas para realizar este proceso por favor contáctame. Recuerda que este pago es un requisito para poder despachar o retirar tu carga.';
                                                                }else{
                                                                    texto1+=' ya se encuentra disponible para pago. En el documento adjunto encontrarás las instrucciones para realizar el pago. Si tienes dudas para realizar este proceso por favor contáctame. Recuerda que este pago es un requisito para poder despachar o retirar tu carga.';
                                                                }

                                                                texto.push(texto1);
                                                            }else if(nro_notificacion==13){//factura disponible
                                                                tipoAtt={tipo:nro_notificacion,carpeta:proforma.rows[0].n_carpeta};
                                                                asunto='WS Cargo | Factura del servicio disponible para descarga';
                                                                texto1='La factura del servicio N° '+resultP.rows[i].fk_consolidado;

                                                                if(resultP.rows[i].referencia!=null && resultP.rows[i].referencia.length>0){
                                                                    texto1+=' | '+resultP.rows[i].referencia+' ya se encuentra disponible para descarga.';
                                                                }else{
                                                                    texto1+=' ya se encuentra disponible para descarga.';
                                                                }

                                                                texto.push(texto1);texto.push('En el archivo adjunto puedes encontrar la factura correspondiente.');
                                                            }



                                                        /* var estadoCorreo = await enviarEmail.mail_notificacion_1({
                                                                asunto:asunto,
                                                                nombreUsuario:res2.data.razon_social.toUpperCase(),
                                                                texto:texto,
                                                                fecha:moment().format('DD/MM/YYYY'),
                                                                attachments:tipoAtt,
                                                                email:resultP.rows[i].email,
                                                                comercial:comercial
                                                            });*/
                                                            let envio=await emailHandler.insertEmailQueue({
                                                                para:resultP.rows[i].email,
                                                                asunto:asunto,
                                                                fecha:moment().format('DD/MM/YYYY'),
                                                                texto:JSON.stringify(texto),
                                                                nombre:res2.data.razon_social.toUpperCase(),
                                                                enlace:null,
                                                                comercial:JSON.stringify(comercial),
                                                                adjunto:tipoAtt,
                                                                tipo:'mail_notificacion_1',
                                                                email_comercial:null,
                                                                datos_adicionales:null,
                                                                datos:null,
                                                                tipo_id:nro_notificacion,
                                                                copia:null,
                                                                copia_oculta:null
                                                            });
                                                        /* var estadoCorreo = await enviarEmail.mail_notificacion_exp_digital({
                                                                asunto:req.body.asunto,
                                                                texto:'Consolidación de carga - Servicio N° '+resultP.rows[i].fk_servicio,
                                                                fecha:moment().format('DD/MM/YYYY'),
                                                                email:resultP.rows[i].email
                                                            });*/
                                                        }
                                                    }
                                                }
                                        });
                                    }
                                }
                            }
                        }
                        }

                    }

                    async function enviar_documentacion_factura(fk_nota_cobro, n_carpeta)
                    {
                        console.log(`ENVIANDO DOCUMENTACION A CLIENTE`);

                        let Lista = await client.query(`

                            SELECT pro.din, c.id as fk_cliente,
                            d.contenedor,
                            c."dteEmail", com.email,
                            concat(com.nombre, ' ', com.apellidos) as nombre,
                            c."razonSocial",
                            com.telefono,
                            (select ct.fk_consolidado
                                from tracking t
                                inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
                                where fk_contenedor = (
                                select id from contenedor c
                                where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1) as fk_servicio,
                            (select referencia from gc_propuestas_cabeceras where fk_servicio = (select ct.fk_consolidado
                                from tracking t
                                inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
                                where fk_contenedor = (
                                select id from contenedor c
                                where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1) limit 1) as n_referencia
                            FROM public.notas_cobros pro
                            INNER JOIN public.despachos d
                                ON CASE
                                    WHEN pro.fk_despacho = -1 THEN pro.codigo_unificacion=d.codigo_unificacion
                                    ELSE pro.fk_despacho = d.id END
                            LEFT JOIN public.notas_cobros_estados nc
                                ON nc.fk_provision = pro.id
                            LEFT JOIN public.clientes c
                                ON c.id = d.fk_cliente
                            LEFT JOIN public.usuario com
                                ON c.fk_comercial=com.id

                            WHERE pro.estado <> false AND pro.id = '${fk_nota_cobro}'
                            LIMIT 1

                        `);

                        var correo_cli = Lista.rows[0]['dteEmail'];
                        var correo_com = Lista.rows[0]['email'];

                        if ( correo_cli == '' ||  correo_cli == undefined ||  correo_cli == null) {
                            console.log("CLIENTE NO TIENE CORREO DE CONTACTO");
                        } else if ( correo_com == '' ||  correo_com == undefined ||  correo_com == null) {
                            console.log("CLIENTE NO TIENE CORREO DE COMERCIAL");
                        } else {

                            let asunto = `WS Cargo | Documentación de tu servicio N°${Lista.rows[0].fk_servicio}`;
                            if (Lista.rows[0].n_referencia != null) {
                                asunto += `/${Lista.rows[0].n_referencia}`
                            }

                            let info_extra = {
                                razon_social: Lista.rows[0]['razonSocial'],
                                fk_servicio: Lista.rows[0].fk_servicio,
                                n_referencia: Lista.rows[0].n_referencia,
                                contenedor: Lista.rows[0].contenedor,
                            }

                            /*
                            let info_comercial = {
                                nombre: Lista.rows[0].nombre,
                                telefono: Lista.rows[0].telefono
                            }
                            */

                            let info_comercial=null;
                            let comer = await funcionesCompartidasCtrl.get_comercial_vigente(Lista.rows[0].fk_cliente);
                            if(comer && comer.rows && comer.rows.length>0){
                                info_comercial=comer.rows[0];
                            }

                            let files = {
                                nc_file: n_carpeta +'.pdf',
                                nc_file_path: 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/notas_de_cobro/'+n_carpeta.substring(0, 7) + '/' + n_carpeta +'.pdf',
                                din_file: 'DIN'+Lista.rows[0].din+'.pdf',
                                din_file_path: 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/din/DIN'+Lista.rows[0].din+'.pdf',
                                fact_file: 'FACTURA_'+n_carpeta+'.pdf',
                                fact_file_path: 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/fact_cargar_por_contenedor/'+n_carpeta+'.pdf',
                                explicativo_file: 'INFORMATIVO.pdf',
                                explicativo_file_path: 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/documento_explicativo.pdf'
                            }

                            /**         REVISO SI EXISTEN MAS DOCUMENTOS            **/
                            if (fs.existsSync('C:/Users/Administrator/Documents/wscargo/restserver/public/files/facturas_cws/PALLET_'+n_carpeta+'.pdf')) {
                                files['cws_pallet_file'] = 'PALLET_'+n_carpeta+'.pdf';
                                files['cws_pallet_file_path'] = 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/facturas_cws/PALLET_'+n_carpeta+'.pdf';
                            }

                            if (fs.existsSync('C:/Users/Administrator/Documents/wscargo/restserver/public/files/facturas_cws/TVP_'+n_carpeta+'.pdf')) {
                                files['cws_tvp_file'] = 'TVP_'+n_carpeta+'.pdf';
                                files['cws_tvp_file_path'] = 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/facturas_cws/TVP_'+n_carpeta+'.pdf';
                            }

                            if (fs.existsSync('C:/Users/Administrator/Documents/wscargo/restserver/public/files/facturas_cws/OTROS_'+n_carpeta+'.pdf')) {
                                files['cws_otros_file'] = 'OTROS_'+n_carpeta+'.pdf';
                                files['cws_otros_file_path'] = 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/facturas_cws/OTROS_'+n_carpeta+'.pdf';
                            }

                            if (fs.existsSync('C:/Users/Administrator/Documents/wscargo/restserver/public/files/facturas_agencia/FA'+Lista.rows[0].din+'.pdf')) {
                                files['agencia_file'] = 'AGENCIA_'+Lista.rows[0].din+'.pdf';
                                files['agencia_file_path'] = 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/facturas_agencia/FA'+Lista.rows[0].din+'.pdf';
                            }

                            if (fs.existsSync('C:/Users/Administrator/Documents/wscargo/restserver/public/files/tgr/TGR'+Lista.rows[0].din+'.pdf')) {
                                files['tgr_file'] = 'TGR_'+Lista.rows[0].din+'.jpg';
                                files['tgr_file_path'] = 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/tgr/TGR'+Lista.rows[0].din+'.jpg';
                            }


                            let envio=await emailHandler.insertEmailQueue({
                                para:correo_cli,
                                asunto:asunto,
                                fecha:null,
                                texto:null,
                                nombre:null,
                                enlace:null,
                                comercial:JSON.stringify(info_comercial),
                                adjunto:JSON.stringify(files),
                                tipo:'mail_factura_nota_de_cobro',
                                email_comercial:null,
                                datos_adicionales:JSON.stringify(info_extra),
                                datos:null,
                                tipo_id:null,
                                copia:correo_com + ', gestion@wscargo.cl, pagos@wscargo.cl, tomas.godoy@wscargo.cl, marcela.illanes@wscargo.cl',
                                copia_oculta:null
                            });
                        }
                    }
                }
            }

        } else {

        }
    }

    async function enviar_documento_maximise_comer(tarea) {
        console.log('Envio de documento comercializadora')
        client.query(` UPDATE public.queue_maximise SET estado='PROCESANDO' WHERE id=${tarea.id} `);

        var moment = require('moment');

        var today = new Date();
        var today_ano = today.getFullYear()*1;
        var today_mes = today.getMonth()+1; if(today_mes<10) { today_mes = '0'+today_mes; }
        var today_dia = today.getDate()*1; if(today_dia<10) { today_dia = '0'+today_dia; }
        var today_hora = (today.getHours()*1) - 4; if(today_hora<10) { today_hora = '0'+today_hora; }
        var today_minuto = today.getMinutes()*1; if(today_minuto<10) { today_minuto = '0'+today_minuto; }

        var facturas = await client.query(` SELECT * FROM public.wsc_envio_facturas_cabeceras2 where id=${tarea['fk_cabecera']}`);

        if(facturas.rows.length>0) {

            let fk_nota_cobro = facturas.rows[0]['fk_nota_cobro'];

            var detalles = await client.query(`
            SELECT
            *
            FROM public.wsc_envio_facturas_detalles2
            where
            fk_cabecera=`+facturas.rows[0]['numero_unico']+`
            and anomesdiahora=`+facturas.rows[0]['anomesdiahora']+`
            and line_text = '${facturas.rows[0]['ref_text2']}'
            `);

            if(detalles.rows.length>0)
            {
                if (facturas.rows[0]['total_amt'] > 0) {
                    var inv_date_aux = facturas.rows[0]['inv_date'].split('-');
                    var contador = await client.query(`
                        SELECT count(*)+1 as cont
                        FROM wsc_envio_facturas_cabeceras2
                        WHERE doc_code='${facturas.rows[0]['doc_code']}' AND fk_nota_cobro=${fk_nota_cobro}
                    `);

                    let monto = Number(facturas.rows[0]['total_amt']).toFixed(0);
                    var flag_refactura =false;
                    var flag_pago_duplicado = false;


                    // PREGUNTO A MAXIMISE SI HAY ALGO QUE YA ESTE PAGADO
                    const request = require('request');
                    const fs = require("fs");

                    const xmltext = `
                                        SELECT ih.INV_NUM, ih.doc_code, ih.cust_code, ih.PAID_AMT, ih.TOTAL_AMT,ih.REF_TEXT1,ih.REF_TEXT2, ih.ext_num,
                                        (SELECT top 1 TOTAL_AMT FROM credithead ch WHERE ch.inv_num = ih.inv_num AND POSTED_FLAG<>'V' AND CMPY_CODE='03') as total_credito
                                        FROM invoicehead ih
                                        WHERE ih.cmpy_code='03' and
                                            ih.POSTED_FLAG<>'V' and
                                            ih.ref_text1='${facturas.rows[0]['ref_text1']}' and
                                            ih.ref_text2='${facturas.rows[0]['ref_text2']}' and
                                            (ih.doc_code='${facturas.rows[0]['doc_code']}' or ih.doc_code='D1')
                                        ORDER BY ih.inv_num DESC;
                                    `;

                    const data_get = {
                        AliasName: 'dwi_tnm',
                        UserName: 'webservice',
                        Password: 'webservice001',
                        Sql: xmltext
                    };

                    const options_get = {
                        url: 'http://asp3.maximise.cl/wsv/query.asmx/GetQueryAsDataSet',
                        method: 'POST',
                        headers: {
                            'Content-Type': 'text/html',
                            'Content-Length': data_get.length
                        },
                        json: true,
                        form: {
                            AliasName: 'dwi_tnm',
                            UserName: 'webservice',
                            Password: 'webservice001',
                            Sql: xmltext
                        }
                    };


                    request.post(options_get, (err, res, body) => {

                        if(err)
                        {
                            console.log('ERROR GETQUERY AS DATASET');
                        }
                        else if(body)
                        {
                            console.log(" RESPONSE GET INVOICEHEAD ");
                            if(JSON.stringify(res.body).includes("System.Web.Services.Protocols.SoapException:")){

                                console.log("System.Web.Services.Protocols.SoapException:");
                                console.log(JSON.stringify(res.body));

                            }
                            else if(res.statusCode==200 )
                            {
                                var entries = JSON.stringify(body).split('<Table');

                                var vistos = {};

                                for(var i=1; i<entries.length; i++) {

                                    console.log(entries[i]);

                                    let inv_num = entries[i].split('<INV_NUM>').pop().split('</INV_NUM>')[0];
                                    let doc_code = entries[i].split('<doc_code>').pop().split('</doc_code>')[0];
                                    let cust_code = entries[i].split('<cust_code>').pop().split('</cust_code>')[0];
                                    let paid_amt = entries[i].split('<PAID_AMT>').pop().split('</PAID_AMT>')[0];
                                    let total_amt = entries[i].split('<TOTAL_AMT>').pop().split('</TOTAL_AMT>')[0];
                                    let ref_text1 = entries[i].split('<REF_TEXT1>').pop().split('</REF_TEXT1>')[0];
                                    let ref_text2 = entries[i].split('<REF_TEXT2>').pop().split('</REF_TEXT2>')[0];
                                    let ext_num = entries[i].split('<ext_num>').pop().split('</ext_num>')[0];
                                    let total_credito = entries[i].split('<total_credito>').pop().split('</total_credito>')[0];
                                    let cred_num = entries[i].split('<cred_num>').pop().split('</cred_num>')[0];

                                    if (doc_code == 'D1') {
                                        monto = monto + Number(total_amt);
                                    } else {
                                        monto = monto - Number(total_amt);
                                        try {
                                            if (Number(total_credito) > 0) {
                                                monto = monto + Number(total_credito);
                                            }
                                        } catch {

                                        }
                                    }

                                    flag_refactura = true;

                                }

                                if (monto > 0) {
                                    enviar_documento_maximise(facturas, inv_date_aux, today_ano, today_mes, today_dia, monto, contador, i, detalles, flag_refactura);
                                }
                            }
                        }
                    });



                    async function enviar_documento_maximise(facturas, inv_date_aux, today_ano, today_mes, today_dia, monto, contador, i, detalles, flag_refactura)
                    {

                        var xmltext_aux = `<string xmlns="Maximise"><CUSTOMERINVOICE><HEAD>`;
                        xmltext_aux += `<CMPY_CODE>`+facturas.rows[0]['cmpy_code']+`</CMPY_CODE>`;
                        xmltext_aux += `<TAX_CODE>IDF</TAX_CODE>`;
                        xmltext_aux += `<SALE_CODE>N/A</SALE_CODE>`;
                        xmltext_aux += `<BRAN_CODE>1</BRAN_CODE>`;
                        xmltext_aux += `<CUST_CODE>`+facturas.rows[0]['cust_code']+`</CUST_CODE>`;
                        xmltext_aux += `<DOC_CODE>`+facturas.rows[0]['doc_code']+`</DOC_CODE>`;
                        xmltext_aux += `<INV_DATE>`+moment(inv_date_aux[2]+"-"+inv_date_aux[1]+"-"+inv_date_aux[0]+" 00:00:00").format('YYYY-MM-DD HH:mm')+`</INV_DATE>`;
                        xmltext_aux += `<ENTRY_CODE>webservice</ENTRY_CODE>`;
                        xmltext_aux += `<ENTRY_DATE>`+moment(today_ano+"-"+today_mes+"-"+today_dia+" 00:00:00").format('YYYY-MM-DD HH:mm')+`</ENTRY_DATE>`;
                        xmltext_aux += `<TERM_CODE>`+facturas.rows[0]['term_code']+`</TERM_CODE>`;
                        xmltext_aux += `<TAX_PER>19</TAX_PER>`;
                        xmltext_aux += `<RATE_EXCHANGE>1</RATE_EXCHANGE>`;

                        if (flag_refactura) {

                            console.log('REFACTURA, monto: ' + monto)
                            var montoSinIva = Number(monto*100/119).toFixed(0);
                            var iva = monto - montoSinIva;

                            xmltext_aux += `<GOODS_AMT>`+montoSinIva+`</GOODS_AMT>`;
                            xmltext_aux += `<EXCENT_AMT>`+facturas.rows[0]['excent_amt']+`</EXCENT_AMT>`;
                            xmltext_aux += `<TOTAL_AMT>`+monto+`</TOTAL_AMT>`;
                            xmltext_aux += `<THRDUE_AMT>`+0+`</THRDUE_AMT>`;
                            xmltext_aux += `<COST_AMT>0</COST_AMT>`;
                            xmltext_aux += `<PAID_AMT>0</PAID_AMT>`;
                            xmltext_aux += `<TAX_AMT>`+iva+`</TAX_AMT>`;

                            var comuna = String(facturas.rows[0]['idcomuna']).length==5?String(facturas.rows[0]['idcomuna']):'0'+String(facturas.rows[0]['idcomuna']);
                            var ciudad = String(facturas.rows[0]['idciudad']);

                            xmltext_aux += `<DUE_DATE>`+facturas.rows[0]['due_date']+`</DUE_DATE>`;
                            xmltext_aux += `<POSTED_FLAG>N</POSTED_FLAG>`;
                            xmltext_aux += `<PRINTED_FLAG>N</PRINTED_FLAG>`;
                            xmltext_aux += `<STORY_FLAG>N</STORY_FLAG>`;
                            xmltext_aux += `<SHIP_CODE>`+facturas.rows[0]['cust_code']+`</SHIP_CODE>`;
                            xmltext_aux += `<NAME_TEXT>`+facturas.rows[0]['name_text']+`</NAME_TEXT>`;
                            xmltext_aux += `<ADDR_TEXT>`+facturas.rows[0]['addr_text']+`</ADDR_TEXT>`;
                            xmltext_aux += `<IDCOMUNA>`+comuna+`</IDCOMUNA>`;
                            xmltext_aux += `<IDCIUDAD>`+ciudad+`</IDCIUDAD>`;
                            xmltext_aux += `<IDDISTRITO>`+comuna+`</IDDISTRITO>`;
                            xmltext_aux += `<IDPAIS>`+facturas.rows[0]['idpais']+`</IDPAIS>`;
                            xmltext_aux += `<BILL_CODE>`+facturas.rows[0]['cust_code']+`</BILL_CODE>`;
                            xmltext_aux += `<BILL_NAME_TEXT>`+facturas.rows[0]['name_text']+`</BILL_NAME_TEXT>`;
                            xmltext_aux += `<BILL_ADDR_TEXT>`+facturas.rows[0]['addr_text']+`</BILL_ADDR_TEXT>`;
                            xmltext_aux += `<BILL_IDCOMUNA>`+comuna+`</BILL_IDCOMUNA>`;
                            xmltext_aux += `<BILL_IDCIUDAD>`+ciudad+`</BILL_IDCIUDAD>`;
                            xmltext_aux += `<BILL_IDDISTRITO>`+comuna+`</BILL_IDDISTRITO>`;
                            xmltext_aux += `<BILL_IDPAIS>`+facturas.rows[0]['idpais']+`</BILL_IDPAIS>`;
                            xmltext_aux += `<COM_TEXT>`+facturas.rows[0]['com_text']+`</COM_TEXT>`;
                            xmltext_aux += `<CURRENCY_CODE>`+facturas.rows[0]['currency_code']+`</CURRENCY_CODE>`;
                            xmltext_aux += `<AR_ACCT_CODE>`+facturas.rows[0]['ar_acct_code']+`</AR_ACCT_CODE>`;
                            xmltext_aux += `<YEAR_NUM>`+(Number(inv_date_aux[2])*1)+`</YEAR_NUM>`;
                            xmltext_aux += `<PERIOD_NUM>`+(Number(inv_date_aux[1])*1)+`</PERIOD_NUM>`;
                            xmltext_aux += `<EXT_NUM>0</EXT_NUM>`;
                            xmltext_aux += `<COLECT_FLAG>S</COLECT_FLAG>`;
                            xmltext_aux += `<COST_POST_FLAG>N</COST_POST_FLAG>`;
                            xmltext_aux += `<FIRST_DUE_DATE>`+facturas.rows[0]['due_date']+`</FIRST_DUE_DATE>`;
                            xmltext_aux += `<PROFIT_CODE>`+facturas.rows[0]['profit_code']+`</PROFIT_CODE>`;
                            xmltext_aux += `<ANALYSIS_TEXT>`+facturas.rows[0]['analysis_text']+`</ANALYSIS_TEXT>`;
                            xmltext_aux += `<REF_TEXT1>`+facturas.rows[0]['ref_text1']+`</REF_TEXT1>`;
                            xmltext_aux += `<REF_TEXT2>`+facturas.rows[0]['ref_text2']+`</REF_TEXT2>`;
                            xmltext_aux += `<REFRATE_EXCHANGE>1</REFRATE_EXCHANGE>`;
                            xmltext_aux += `<RETTAX_AMT>0</RETTAX_AMT>`;
                            xmltext_aux += `<SELTAX_AMT>0</SELTAX_AMT>`;
                            xmltext_aux += `<DISC_IND>P</DISC_IND>`;
                            xmltext_aux += `<RET_AMT>0</RET_AMT>`;
                            xmltext_aux += `<RET_PER>0</RET_PER>`;
                            xmltext_aux += `</HEAD>`;

                            var NombreArchivo = facturas.rows[0]['ref_text1'];

                            xmltext_aux += `<DETAIL>`;
                            xmltext_aux += `<CMPY_CODE>`+facturas.rows[0]['cmpy_code']+`</CMPY_CODE>`;
                            xmltext_aux += `<LINE_NUM>`+1+`</LINE_NUM>`;
                            xmltext_aux += `<PART_CODE>`+detalles.rows[0]['part_code']+`</PART_CODE>`;
                            xmltext_aux += `<WARE_CODE>`+detalles.rows[0]['ware_code']+`</WARE_CODE>`;
                            xmltext_aux += `<UOM_CODE>`+detalles.rows[0]['uom_code']+`</UOM_CODE>`;
                            xmltext_aux += `<UOM_QTY>`+detalles.rows[0]['uom_qty']+`</UOM_QTY>`;
                            xmltext_aux += `<ORD_QTY>`+detalles.rows[0]['qrd_qty']+`</ORD_QTY>`;
                            xmltext_aux += `<LINE_TEXT>`+detalles.rows[0]['line_text']+`</LINE_TEXT>`;
                            xmltext_aux += `<UNIT_COST_AMT>`+detalles.rows[0]['unit_cost_amt']+`</UNIT_COST_AMT>`;
                            xmltext_aux += `<EXT_COST_AMT>`+detalles.rows[0]['ext_cost_amot']+`</EXT_COST_AMT>`;
                            xmltext_aux += `<DISC_PER>`+detalles.rows[0]['disc_per']+`</DISC_PER>`;
                            xmltext_aux += `<UNIT_SALE_AMT>`+montoSinIva+`</UNIT_SALE_AMT>`;
                            xmltext_aux += `<EXT_SALE_AMT>`+montoSinIva+`</EXT_SALE_AMT>`;
                            xmltext_aux += `<UNIT_EXCENT_AMT>`+detalles.rows[0]['unit_excent_amt']+`</UNIT_EXCENT_AMT>`;
                            xmltext_aux += `<EXT_EXCENT_AMT>`+detalles.rows[0]['ext_excent_amt']+`</EXT_EXCENT_AMT>`;
                            xmltext_aux += `<UNIT_TAX_AMT>`+iva+`</UNIT_TAX_AMT>`;
                            xmltext_aux += `<EXT_TAX_AMT>`+iva+`</EXT_TAX_AMT>`;
                            xmltext_aux += `<LINE_TOTAL_AMT>`+monto+`</LINE_TOTAL_AMT>`;
                            xmltext_aux += `<LIST_PRICE_AMT>`+detalles.rows[0]['list_price_amt']+`</LIST_PRICE_AMT>`;
                            xmltext_aux += `<CUST_PRICE_AMT>`+montoSinIva+`</CUST_PRICE_AMT>`;
                            xmltext_aux += `<LINE_ACCT_CODE>`+detalles.rows[0]['line_acct_code']+`</LINE_ACCT_CODE>`;
                            xmltext_aux += `<LEVEL_CODE>`+detalles.rows[0]['level_code']+`</LEVEL_CODE>`;
                            xmltext_aux += `<BACK_QTY>`+detalles.rows[0]['back_qty']+`</BACK_QTY>`;
                            xmltext_aux += `<PENDING_FLAG>`+detalles.rows[0]['pending_flag']+`</PENDING_FLAG>`;
                            xmltext_aux += `<UNIT_COST_AMT1>`+detalles.rows[0]['unit_cost_amt1']+`</UNIT_COST_AMT1>`;
                            xmltext_aux += `<SHIP_DATE>`+detalles.rows[0]['ship_date']+`</SHIP_DATE>`;
                            xmltext_aux += `<PROFIT_CODE>`+detalles.rows[0]['profit_code']+`</PROFIT_CODE>`;
                            xmltext_aux += `<BRAN_CODE>1</BRAN_CODE>`;
                            xmltext_aux += `<UNIT_COMCOST_AMT>`+detalles.rows[0]['unit_comcost_amt']+`</UNIT_COMCOST_AMT>`;
                            xmltext_aux += `<UNIT_IFRSCOST_AMT>`+detalles.rows[0]['unit_ifrscost_amt']+`</UNIT_IFRSCOST_AMT>`;
                            xmltext_aux += `<REF_TEXT1>`+facturas.rows[0]['ref_text1']+`</REF_TEXT1>`;
                            xmltext_aux += `<REF_TEXT2>`+''+`</REF_TEXT2>`;
                            xmltext_aux += `<TAXEDUNIT_PRICE_AMT>`+monto+`</TAXEDUNIT_PRICE_AMT>`;
                            xmltext_aux += `</DETAIL>`;

                        } else {

                            xmltext_aux += `<GOODS_AMT>`+facturas.rows[0]['goods_amt']+`</GOODS_AMT>`;
                            xmltext_aux += `<EXCENT_AMT>`+facturas.rows[0]['excent_amt']+`</EXCENT_AMT>`;
                            xmltext_aux += `<TOTAL_AMT>`+facturas.rows[0]['total_amt']+`</TOTAL_AMT>`;
                            xmltext_aux += `<THRDUE_AMT>`+0+`</THRDUE_AMT>`;
                            xmltext_aux += `<COST_AMT>0</COST_AMT>`;
                            xmltext_aux += `<PAID_AMT>0</PAID_AMT>`;
                            xmltext_aux += `<TAX_AMT>`+facturas.rows[0]['tax_amt']+`</TAX_AMT>`;

                            var comuna = String(facturas.rows[0]['idcomuna']).length==5?String(facturas.rows[0]['idcomuna']):'0'+String(facturas.rows[0]['idcomuna']);
                            var ciudad = String(facturas.rows[0]['idciudad']);

                            xmltext_aux += `<DUE_DATE>`+facturas.rows[0]['due_date']+`</DUE_DATE>`;
                            xmltext_aux += `<POSTED_FLAG>N</POSTED_FLAG>`;
                            xmltext_aux += `<PRINTED_FLAG>N</PRINTED_FLAG>`;
                            xmltext_aux += `<STORY_FLAG>N</STORY_FLAG>`;
                            xmltext_aux += `<SHIP_CODE>`+facturas.rows[0]['cust_code']+`</SHIP_CODE>`;
                            xmltext_aux += `<NAME_TEXT>`+facturas.rows[0]['name_text']+`</NAME_TEXT>`;
                            xmltext_aux += `<ADDR_TEXT>`+facturas.rows[0]['addr_text']+`</ADDR_TEXT>`;
                            xmltext_aux += `<IDCOMUNA>`+comuna+`</IDCOMUNA>`;
                            xmltext_aux += `<IDCIUDAD>`+ciudad+`</IDCIUDAD>`;
                            xmltext_aux += `<IDDISTRITO>`+comuna+`</IDDISTRITO>`;
                            xmltext_aux += `<IDPAIS>`+facturas.rows[0]['idpais']+`</IDPAIS>`;
                            xmltext_aux += `<BILL_CODE>`+facturas.rows[0]['cust_code']+`</BILL_CODE>`;
                            xmltext_aux += `<BILL_NAME_TEXT>`+facturas.rows[0]['name_text']+`</BILL_NAME_TEXT>`;
                            xmltext_aux += `<BILL_ADDR_TEXT>`+facturas.rows[0]['addr_text']+`</BILL_ADDR_TEXT>`;
                            xmltext_aux += `<BILL_IDCOMUNA>`+comuna+`</BILL_IDCOMUNA>`;
                            xmltext_aux += `<BILL_IDCIUDAD>`+ciudad+`</BILL_IDCIUDAD>`;
                            xmltext_aux += `<BILL_IDDISTRITO>`+comuna+`</BILL_IDDISTRITO>`;
                            xmltext_aux += `<BILL_IDPAIS>`+facturas.rows[0]['idpais']+`</BILL_IDPAIS>`;
                            xmltext_aux += `<COM_TEXT>`+facturas.rows[0]['com_text']+`</COM_TEXT>`;
                            xmltext_aux += `<CURRENCY_CODE>`+facturas.rows[0]['currency_code']+`</CURRENCY_CODE>`;
                            xmltext_aux += `<AR_ACCT_CODE>`+facturas.rows[0]['ar_acct_code']+`</AR_ACCT_CODE>`;
                            xmltext_aux += `<YEAR_NUM>`+(Number(inv_date_aux[2])*1)+`</YEAR_NUM>`;
                            xmltext_aux += `<PERIOD_NUM>`+(Number(inv_date_aux[1])*1)+`</PERIOD_NUM>`;
                            xmltext_aux += `<EXT_NUM>0</EXT_NUM>`;
                            xmltext_aux += `<COLECT_FLAG>S</COLECT_FLAG>`;
                            xmltext_aux += `<COST_POST_FLAG>N</COST_POST_FLAG>`;
                            xmltext_aux += `<FIRST_DUE_DATE>`+facturas.rows[0]['due_date']+`</FIRST_DUE_DATE>`;
                            xmltext_aux += `<PROFIT_CODE>`+facturas.rows[0]['profit_code']+`</PROFIT_CODE>`;
                            xmltext_aux += `<ANALYSIS_TEXT>`+facturas.rows[0]['analysis_text']+`</ANALYSIS_TEXT>`;
                            xmltext_aux += `<REF_TEXT1>`+facturas.rows[0]['ref_text1']+`</REF_TEXT1>`;
                            xmltext_aux += `<REF_TEXT2>`+facturas.rows[0]['ref_text2']+`</REF_TEXT2>`;
                            xmltext_aux += `<REFRATE_EXCHANGE>1</REFRATE_EXCHANGE>`;
                            xmltext_aux += `<RETTAX_AMT>0</RETTAX_AMT>`;
                            xmltext_aux += `<SELTAX_AMT>0</SELTAX_AMT>`;
                            xmltext_aux += `<DISC_IND>P</DISC_IND>`;
                            xmltext_aux += `<RET_AMT>0</RET_AMT>`;
                            xmltext_aux += `<RET_PER>0</RET_PER>`;
                            xmltext_aux += `</HEAD>`;

                            var NombreArchivo = facturas.rows[0]['ref_text1'];
                            for(var i=0; i<detalles.rows.length; i++)
                            {
                                xmltext_aux += `<DETAIL>`;
                                xmltext_aux += `<CMPY_CODE>`+facturas.rows[0]['cmpy_code']+`</CMPY_CODE>`;
                                xmltext_aux += `<LINE_NUM>`+1+`</LINE_NUM>`;
                                xmltext_aux += `<PART_CODE>`+detalles.rows[0]['part_code']+`</PART_CODE>`;
                                xmltext_aux += `<WARE_CODE>`+detalles.rows[0]['ware_code']+`</WARE_CODE>`;
                                xmltext_aux += `<UOM_CODE>`+detalles.rows[0]['uom_code']+`</UOM_CODE>`;
                                xmltext_aux += `<UOM_QTY>`+detalles.rows[0]['uom_qty']+`</UOM_QTY>`;
                                xmltext_aux += `<ORD_QTY>`+detalles.rows[0]['qrd_qty']+`</ORD_QTY>`;
                                xmltext_aux += `<LINE_TEXT>`+detalles.rows[0]['line_text']+`</LINE_TEXT>`;
                                xmltext_aux += `<UNIT_COST_AMT>`+detalles.rows[0]['unit_cost_amt']+`</UNIT_COST_AMT>`;
                                xmltext_aux += `<EXT_COST_AMT>`+detalles.rows[0]['ext_cost_amot']+`</EXT_COST_AMT>`;
                                xmltext_aux += `<DISC_PER>`+detalles.rows[0]['disc_per']+`</DISC_PER>`;
                                xmltext_aux += `<UNIT_SALE_AMT>`+detalles.rows[0]['unit_sale_amt']+`</UNIT_SALE_AMT>`;
                                xmltext_aux += `<EXT_SALE_AMT>`+detalles.rows[0]['ext_sale_amt']+`</EXT_SALE_AMT>`;
                                xmltext_aux += `<UNIT_EXCENT_AMT>`+detalles.rows[0]['unit_excent_amt']+`</UNIT_EXCENT_AMT>`;
                                xmltext_aux += `<EXT_EXCENT_AMT>`+detalles.rows[0]['ext_excent_amt']+`</EXT_EXCENT_AMT>`;
                                xmltext_aux += `<UNIT_TAX_AMT>`+detalles.rows[0]['unit_taxt_amt']+`</UNIT_TAX_AMT>`;
                                xmltext_aux += `<EXT_TAX_AMT>`+detalles.rows[0]['ext_tax_amt']+`</EXT_TAX_AMT>`;
                                xmltext_aux += `<LINE_TOTAL_AMT>`+detalles.rows[0]['line_total_amt']+`</LINE_TOTAL_AMT>`;
                                xmltext_aux += `<LIST_PRICE_AMT>`+detalles.rows[0]['list_price_amt']+`</LIST_PRICE_AMT>`;
                                xmltext_aux += `<CUST_PRICE_AMT>`+detalles.rows[0]['cust_price_amt']+`</CUST_PRICE_AMT>`;
                                xmltext_aux += `<LINE_ACCT_CODE>`+detalles.rows[0]['line_acct_code']+`</LINE_ACCT_CODE>`;
                                xmltext_aux += `<LEVEL_CODE>`+detalles.rows[0]['level_code']+`</LEVEL_CODE>`;
                                xmltext_aux += `<BACK_QTY>`+detalles.rows[0]['back_qty']+`</BACK_QTY>`;
                                xmltext_aux += `<PENDING_FLAG>`+detalles.rows[0]['pending_flag']+`</PENDING_FLAG>`;
                                xmltext_aux += `<UNIT_COST_AMT1>`+detalles.rows[0]['unit_cost_amt1']+`</UNIT_COST_AMT1>`;
                                xmltext_aux += `<SHIP_DATE>`+detalles.rows[0]['ship_date']+`</SHIP_DATE>`;
                                xmltext_aux += `<PROFIT_CODE>`+detalles.rows[0]['profit_code']+`</PROFIT_CODE>`;
                                xmltext_aux += `<BRAN_CODE>1</BRAN_CODE>`;
                                xmltext_aux += `<UNIT_COMCOST_AMT>`+detalles.rows[0]['unit_comcost_amt']+`</UNIT_COMCOST_AMT>`;
                                xmltext_aux += `<UNIT_IFRSCOST_AMT>`+detalles.rows[0]['unit_ifrscost_amt']+`</UNIT_IFRSCOST_AMT>`;
                                xmltext_aux += `<REF_TEXT1>`+facturas.rows[0]['ref_text1']+`</REF_TEXT1>`;
                                xmltext_aux += `<REF_TEXT2>`+''+`</REF_TEXT2>`;
                                xmltext_aux += `<TAXEDUNIT_PRICE_AMT>`+detalles.rows[0]['taxedunit_price_amt']+`</TAXEDUNIT_PRICE_AMT>`;
                                xmltext_aux += `</DETAIL>`;
                            }
                        }


                        xmltext_aux +=`</CUSTOMERINVOICE></string>`;
                        console.log(xmltext_aux);

                        const data = {
                            AliasName: 'dwi_tnm',
                            UserName: 'webservice',
                            Password: 'webservice001',
                            Data: xmltext_aux
                        };

                        const options = {
                            url: 'http://asp3.maximise.cl/wsv/Invoice.asmx/SaveDocument',
                            method: 'POST',
                            headers: {
                                'Content-Type': 'text/html',
                                'Content-Length': data.length
                            },
                            json: true,
                            form: {
                                AliasName: 'dwi_tnm',
                                UserName: 'webservice',
                                Password: 'webservice001',
                                Data: xmltext_aux
                            }
                        };
                        request.post(options, (err, res, body) => {

                            var texto_log = '';
                            if(err)
                            {
                                update_error_cargar_maximise(err, facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);
                            }
                            else if(body)
                            {
                                console.log(" --------------------------- RES "+facturas.rows[0]['ref_text1'] + ' -----------------------------');
                                console.log("  ");
                                if(JSON.stringify(res.body).includes("System.Web.Services.Protocols.SoapException:")){

                                    console.log(" --------------------------- ERROR ");
                                    console.log(JSON.stringify(res.body));
                                    update_error_cargar_maximise(JSON.stringify(res.body), facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);

                                }
                                else if(res.statusCode==200 )
                                {
                                    var numero_interno = JSON.stringify(body);
                                    numero_interno = numero_interno.replace('</int>', '');
                                    var id_maximise = numero_interno.substring(71, (Number(numero_interno.length)-1) );
                                    console.log(" --------------------------- BODY REPORT ");
                                    console.log(" --------------------------- "+id_maximise);
                                    console.log("  ");

                                    let numero, msg;
                                    let flag = false;

                                    if (facturas.rows[0]['doc_code'] == 'DI') {
                                        numero = 12;
                                        msg = 'Pago del IVA disponible';
                                        flag = true;
                                    } else if (facturas.rows[0]['doc_code'] == 'F8' || facturas.rows[0]['doc_code'] == 'P8') {
                                        numero = 11;
                                        msg = 'Pago del servicio disponible';
                                        flag = true;
                                    }

                                    if (flag) {
                                        enviar_notificacion(numero, msg,facturas.rows[0]['fk_nota_cobro']);
                                    }

                                    update_cargado_maximise(id_maximise, facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);
                                    if( facturas.rows[0]['servicio']=='SI' ){
                                        enviar_documento_sii(id_maximise, facturas.rows[0]['numero_unico'], facturas.rows[0]['anomesdiahora'], NombreArchivo, facturas.rows[0]['cmpy_code'], facturas.rows[0]['fk_nota_cobro'], facturas.rows[0]['doc_code']);
                                    }
                                }
                            }
                        });


                    }

                    async function enviar_documento_sii(id_maximise, cabecera, anomesdiahora, NombreArchivo, cmpy_code, fk_nota_cobro, doc_code)
                    {
                        update_estado_factura('ENVIANDO AL SII', cabecera, anomesdiahora, fk_nota_cobro, doc_code);

                        const data = {
                            AliasName: 'dwi_tnm',
                            UserName: 'webservice',
                            Password: 'webservice001',
                            DocumentNumber: id_maximise,
                            Company: cmpy_code
                        };

                        const options = {
                            url: 'http://asp3.maximise.cl/wsv/Invoice.asmx/PrintDocument',
                            method: 'POST',
                            headers: {
                                'Content-Type': 'text/html',
                                'Content-Length': data.length
                            },
                            json: true,
                            form: {
                                AliasName: 'dwi_tnm',
                                UserName: 'webservice',
                                Password: 'webservice001',
                                DocumentNumber: id_maximise,
                                Company: cmpy_code
                            }
                        };

                        request.post(options, (err, res, body) => {

                            var texto_log = '';
                            if(err)
                            {
                                update_error_cargar_sii('ERROR CARGAR SII', cabecera, anomesdiahora, fk_nota_cobro, doc_code);
                            }
                            else if(body)
                            {
                                console.log(" --------------------------- SII --------------------------");
                                console.log("  ");
                                if(res.statusCode==200 )
                                {
                                    console.log(" --------------------------- BODY REPORT "+JSON.stringify(body));
                                    console.log("  ");

                                    var folio_sii = JSON.stringify(body);
                                    folio_sii = folio_sii.replace('</string>"', '');
                                    folio_sii = folio_sii.substring(74, (Number(folio_sii.length)) );

                                    actualizar_sii_factura (folio_sii, cabecera, anomesdiahora, fk_nota_cobro, doc_code);

                                    const data = {
                                        AliasName: 'dwi_tnm',
                                        UserName: 'webservice',
                                        Password: 'webservice001',
                                        DocumentNumber: id_maximise,
                                        Company: cmpy_code
                                    };


                                    const options = {
                                        url: 'http://asp3.maximise.cl/wsv/Invoice.asmx/DownloadPdf',
                                        method: 'POST',
                                        headers: {
                                            'Content-Type': 'text/html',
                                            'Content-Length': data.length
                                        },
                                        json: true,
                                        form: {
                                            AliasName: 'dwi_tnm',
                                            UserName: 'webservice',
                                            Password: 'webservice001',
                                            DocumentNumber: id_maximise,
                                            Company: cmpy_code
                                        }
                                    };

                                    const response_req = request.post(options, (err, res, body) => {

                                    });

                                    response_req.on('response', function (res) {

                                        res.pipe(fs.createWriteStream('C:/Users/Administrator/Documents/wscargo/restserver/public/files/fact_cargar_por_contenedor/'+NombreArchivo+'.pdf'));

                                    });

                                    enviar_notificacion(13, 'Factura diponible del servicio disponible para entrega',fk_nota_cobro);


                                    return "OK";

                                }
                                else
                                {
                                    return "ERROR2";
                                }
                            }

                        });
                    }

                    async function update_cargado_maximise(id_maximise, numero_unico, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log (`CARGADA MAXIMISE `);

                        await client.query(`
                        UPDATE public.wsc_envio_facturas_cabeceras2
                        SET estado='CARGADA MAXIMISE'
                        , max_id_interno=`+id_maximise+`
                        where
                        numero_unico=`+numero_unico+`
                        and
                        anomesdiahora=`+anomesdiahora+`
                        `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(`
                        UPDATE public.notas_cobros_estados
                        SET estado='CARGADA MAXIMISE'
                        WHERE fk_provision=`+fk_nota_cobro+` AND fk_tipo=${codigo}`);

                        client.query(` UPDATE public.queue_maximise SET estado='CARGADA MAXIMISE' WHERE id=${tarea.id} `);

                    }

                    async function update_error_cargar_sii(err, numero_unico, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log(` ERROR CARGAR SII`);

                        await client.query(`
                        UPDATE public.wsc_envio_facturas_cabeceras2
                        SET
                        estado='ERROR CARGAR SII'
                        , log = concat(log,'\n','`+err+`')
                        where
                        numero_unico=`+numero_unico+`
                        and
                        anomesdiahora=`+anomesdiahora+`
                        `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(`
                        UPDATE public.notas_cobros_estados
                        SET estado='ERROR CARGAR SII'
                        WHERE fk_provision=`+fk_nota_cobro+`  AND fk_tipo=${codigo}`);

                        client.query(` UPDATE public.queue_maximise SET estado='ERROR CARGAR SII', info_extra='ERROR: ${err}' WHERE id=${tarea.id} `);

                    }

                    async function update_error_cargar_maximise(err, numero_unico, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log(`ERROR CARGAR MAXIMISE`);

                        console.log(`
                        UPDATE public.wsc_envio_facturas_cabeceras2
                        SET
                        estado='ERROR CARGAR MAXIMISE'
                        , log = concat(log,'\n','`+err+`')
                        where
                        numero_unico=`+numero_unico+`
                        and
                        anomesdiahora=`+anomesdiahora+`
                        `);

                        await client.query(`
                        UPDATE public.wsc_envio_facturas_cabeceras2
                        SET
                        estado='ERROR CARGAR MAXIMISE'
                        , log = concat(log,'\n','`+err+`')
                        where
                        numero_unico=`+numero_unico+`
                        and
                        anomesdiahora=`+anomesdiahora+`
                        `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(`
                        UPDATE public.notas_cobros_estados
                        SET estado='ERROR CARGAR MAXIMISE'
                        WHERE fk_provision=`+fk_nota_cobro+` AND fk_tipo=${codigo}`);

                        client.query(` UPDATE public.queue_maximise SET estado='ERROR CARGAR MAXIMISE', info_extra='ERROR: ${err} WHERE id=${tarea.id} `);

                    }

                    async function update_estado_factura(estado, cabecera, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log(`estado`);
                        await client.query(` UPDATE public.wsc_envio_facturas_cabeceras2 SET estado='`+estado+`' where numero_unico=`+cabecera+` and anomesdiahora=`+anomesdiahora+` `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(` UPDATE public.notas_cobros_estados SET estado='${estado}' WHERE fk_provision=`+fk_nota_cobro+` AND fk_tipo=${codigo} `);

                        client.query(` UPDATE public.queue_maximise SET estado='${estado}' WHERE id=${tarea.id} `);

                    }

                    async function actualizar_sii_factura(folio_sii, cabecera, anomesdiahora, fk_nota_cobro, doc_code)
                    {
                        console.log(`ENVIADA SII`);
                        await client.query(` UPDATE public.wsc_envio_facturas_cabeceras2 SET estado='ENVIADA SII', sii_factura='`+folio_sii+`' where numero_unico=`+cabecera+` and anomesdiahora=`+anomesdiahora+` `);

                        var codigo = doc_code=='DI'?1:doc_code=='P8'||doc_code=='PF'?2:3;
                        await client.query(` UPDATE public.notas_cobros_estados SET estado='ENVIADA SII' WHERE fk_provision=`+fk_nota_cobro+` AND fk_tipo=${codigo}`);

                        client.query(` UPDATE public.queue_maximise SET estado='ENVIADA SII' WHERE id=${tarea.id} `);


                    }

                    async function enviar_notificacion(nro_notificacion, message,fk_nota_cobro)
                    {

                        if(process.env.notificacionesClienteDigital){
                            let proforma=await client.query(`
                            SELECT
                                d.id,d.contenedor,d.fk_cliente,c.id as fk_contenedor,cp.id as fk_proforma,d.n_carpeta from
                                public.despachos d
                                INNER JOIN public.notas_cobros nc on nc.fk_despacho=d.id
                                INNER JOIN public.contenedor c on c.codigo=d.contenedor
                                INNER JOIN public.contenedor_proforma cp on cp.fk_contenedor=c.id
                                where nc.id=`+fk_nota_cobro+` and cp.estado>=0`);

                            if(proforma && proforma.rows && proforma.rows.length>0){
                            let resultP=await client.query(`
                            SELECT
                            t.fk_propuesta,t.fk_cliente,gc.fk_servicio,gc.referencia,ct.fk_consolidado,
                            upper(c."dteEmail") as email
                            FROM
                            public.contenedor_proforma_detalle cpd
                            INNER JOIN public.tracking_detalle td on td.id=cpd.fk_tracking_detalle
                            INNER JOIN public.tracking t on t.id=td.tracking_id
                            INNER JOIN public.consolidado_tracking ct on ct.fk_tracking=t.id
                            INNER JOIN public.gc_propuestas_cabeceras gc on gc.id=t.fk_propuesta
                            INNER JOIN public.clientes c on c.id=t.fk_cliente
                            WHERE cpd.fk_contenedor_proforma=`+proforma.rows[0].fk_proforma+` and t.fk_cliente=`+proforma.rows[0].fk_cliente+` group by t.fk_propuesta,t.fk_cliente,gc.fk_servicio,c."dteEmail",gc.referencia,ct.fk_consolidado`);

                            if(resultP && resultP.rows && resultP.rows.length>0){
                                for(let i=0;i<resultP.rows.length;i++){
                                    if(resultP.rows[i].fk_servicio!=null && resultP.rows[i].fk_servicio>0){
                                        let NotifConfig1=await Notifications.verifyConfigNotificationExpDigital(resultP.rows[i].fk_cliente, nro_notificacion/*NUMERO DE NOTIFICACION*/,
                                            async function(res2){
                                                if(res2!=null && res2.data){
                                                    if(res2.data.estado_web===true){
                                                        let insertNotification1=await Notifications.insertNotificationExpDigital({
                                                            fk_cliente:resultP.rows[i].fk_cliente,
                                                            texto:message,
                                                            fk_notificacion_configuracion:res2.data.id,
                                                            visto:false,
                                                            fk_servicio:resultP.rows[i].fk_servicio
                                                        },async function(res3){
                                                            console.log('res3',res3);
                                                        });
                                                    }

                                                    if(res2.data.estado_correo===true){
                                                        if(resultP.rows[i].email && resultP.rows[i].email.length>0){

                                                            let comercial=null;
                                                            let comer = await funcionesCompartidasCtrl.get_comercial_vigente(resultP.rows[i].fk_cliente);
                                                            if(comer && comer.rows && comer.rows.length>0){
                                                                comercial=comer.rows[0];
                                                            }

                                                            /*
                                                            let comercial=null;
                                                            if(res2.data.fk_comercial && res2.data.fk_comercial!=null){
                                                                let comer=await client.query(`SELECT concat(nombre,' ',apellidos) as nombre,email,telefono FROM public.usuario where id=`+res2.data.fk_comercial);
                                                                if(comer && comer.rows && comer.rows.length>0){
                                                                    comercial=comer.rows[0];
                                                                }
                                                            }
                                                            */

                                                            let asunto='';
                                                            let texto1='';
                                                            let texto=['Hola '+res2.data.razon_social.toUpperCase()+'.'];let tipoAtt=null;
                                                            if(nro_notificacion==11){//pago disponible
                                                                tipoAtt={tipo:nro_notificacion,carpeta:proforma.rows[0].n_carpeta};
                                                                asunto='WS Cargo | Pago disponible del servicio N° '+resultP.rows[i].fk_consolidado;
                                                                texto1='El costo del servicio N° '+resultP.rows[i].fk_consolidado;

                                                                if(resultP.rows[i].referencia!=null && resultP.rows[i].referencia.length>0){
                                                                    asunto+=' | '+resultP.rows[i].referencia;
                                                                    texto1+=' | '+resultP.rows[i].referencia+' ya se encuentra disponible para pago. En el documento adjunto encontrarás las instrucciones para realizar el pago. Si tienes dudas para realizar este proceso por favor contáctame. Recuerda que este pago es un requisito para poder despachar o retirar tu carga.';
                                                                }else{
                                                                    texto1+=' ya se encuentra disponible para pago. En el documento adjunto encontrarás las instrucciones para realizar el pago. Si tienes dudas para realizar este proceso por favor contáctame. Recuerda que este pago es un requisito para poder despachar o retirar tu carga.';
                                                                }

                                                                texto.push(texto1);texto.push('En el archivo adjunto puedes encontrar la factura correspondiente.');
                                                            }else if(nro_notificacion==12){//pago iva disponible
                                                                tipoAtt={tipo:nro_notificacion,carpeta:proforma.rows[0].n_carpeta};
                                                                asunto='WS Cargo | Pago disponible del IVA del servicio N° '+resultP.rows[i].fk_consolidado;
                                                                texto1='El costo del IVA de tu servicio N° '+resultP.rows[i].fk_consolidado;

                                                                if(resultP.rows[i].referencia!=null && resultP.rows[i].referencia.length>0){
                                                                    asunto+=' | '+resultP.rows[i].referencia;
                                                                    texto1+=' | '+resultP.rows[i].referencia+' ya se encuentra disponible para pago. En el documento adjunto encontrarás las instrucciones para realizar el pago. Si tienes dudas para realizar este proceso por favor contáctame. Recuerda que este pago es un requisito para poder despachar o retirar tu carga.';
                                                                }else{
                                                                    texto1+=' ya se encuentra disponible para pago. En el documento adjunto encontrarás las instrucciones para realizar el pago. Si tienes dudas para realizar este proceso por favor contáctame. Recuerda que este pago es un requisito para poder despachar o retirar tu carga.';
                                                                }

                                                                texto.push(texto1);
                                                            }else if(nro_notificacion==13){//factura disponible
                                                                tipoAtt={tipo:nro_notificacion,carpeta:proforma.rows[0].n_carpeta};
                                                                asunto='WS Cargo | Factura del servicio disponible para descarga';
                                                                texto1='La factura del servicio N° '+resultP.rows[i].fk_consolidado;

                                                                if(resultP.rows[i].referencia!=null && resultP.rows[i].referencia.length>0){
                                                                    texto1+=' | '+resultP.rows[i].referencia+' ya se encuentra disponible para descarga.';
                                                                }else{
                                                                    texto1+=' ya se encuentra disponible para descarga.';
                                                                }

                                                                texto.push(texto1);texto.push('En el archivo adjunto puedes encontrar la factura correspondiente.');
                                                            }



                                                            /*var estadoCorreo = await enviarEmail.mail_notificacion_1({
                                                                asunto:asunto,
                                                                nombreUsuario:res2.data.razon_social.toUpperCase(),
                                                                texto:texto,
                                                                fecha:moment().format('DD/MM/YYYY'),
                                                                email:resultP.rows[i].email,
                                                                attachments:tipoAtt,
                                                                comercial:comercial
                                                            });*/

                                                            let envio=await emailHandler.insertEmailQueue({
                                                                para:resultP.rows[i].email,
                                                                asunto:asunto,
                                                                fecha:moment().format('DD/MM/YYYY'),
                                                                texto:JSON.stringify(texto),
                                                                nombre:res2.data.razon_social.toUpperCase(),
                                                                enlace:null,
                                                                comercial:JSON.stringify(comercial),
                                                                adjunto:tipoAtt,
                                                                tipo:'mail_notificacion_1',
                                                                email_comercial:null,
                                                                datos_adicionales:null,
                                                                datos:null,
                                                                tipo_id:nro_notificacion,
                                                                copia:null,
                                                                copia_oculta:null
                                                            });
                                                        /* var estadoCorreo = await enviarEmail.mail_notificacion_exp_digital({
                                                                asunto:req.body.asunto,
                                                                texto:'Consolidación de carga - Servicio N° '+resultP.rows[i].fk_servicio,
                                                                fecha:moment().format('DD/MM/YYYY'),
                                                                email:resultP.rows[i].email
                                                            });*/
                                                        }
                                                    }
                                                }
                                        });
                                    }
                                }
                            }
                            }
                        }

                    }
                }
            }

        } else {

        }

    }


}


exports.generarPdfFacturas = async (req, resp) => {
    var sche_generarPdfFacturas = require('node-schedule');

    sche_generarPdfFacturas.scheduleJob('*/30 * * * * *', () => {
        console.log("");
        console.log("DESCARGANDO FACTURA CWS");
        generarPdfFacturas();

    });

    async function generarPdfFacturas()
    {

        var facturas = await client.query(` SELECT * FROM public.wsc_envio_facturas_cabeceras2 where cmpy_code='03' AND max_id_interno<>0 and archivofactura='0' limit 1 `);

        if(facturas.rows.length>0)
        {
            console.log(" CONSULTANDO FACTURA " + facturas.rows[0]['ref_text2'] + ' de ' + facturas.rows[0]['ref_text1']);
            update_nombre_archivo(facturas.rows[0]['id'], 'PROCESANDO');

            const request = require('request');
            const fs = require("fs");

            const data = {
                AliasName: 'dwi_tnm',
                UserName: 'webservice',
                Password: 'webservice001',
                DocumentNumber: facturas.rows[0]['max_id_interno'],
                Company: facturas.rows[0]['cmpy_code']
            };

            const options = {
                url: 'http://asp3.maximise.cl/wsv/Invoice.asmx/DownloadPdf',
                method: 'POST',
                headers: {
                    'Content-Type': 'text/html',
                    'Content-Length': data.length
                },
                json: true,
                form: {
                    AliasName: 'dwi_tnm',
                    UserName: 'webservice',
                    Password: 'webservice001',
                    DocumentNumber: facturas.rows[0]['max_id_interno'],
                    Company:  facturas.rows[0]['cmpy_code']
                }
            };

            const response_req = request.post(options, (err, res, body) => {

            });

            response_req.on('response', function (res) {

                res.pipe(fs.createWriteStream('C:/Users/Administrator/Documents/wscargo/restserver/public/files/facturas_cws/'+facturas.rows[0]['ref_text2']+'_'+facturas.rows[0]['ref_text1']+'.pdf'));
                console.log("FACTURA DESCARGADA");
                update_nombre_archivo(facturas.rows[0]['id'], facturas.rows[0]['ref_text2']);

            });
            return "OK";

        } else {
            console.log("NO QUEDAN MAS FACTURAS POR DESCARGAR");

        }

        async function update_nombre_archivo(id, archivo)
        {
            console.log(` UPDATE public.wsc_envio_facturas_cabeceras2 SET archivofactura='`+archivo+`' where id=`+id+` `);
            await client.query(` UPDATE public.wsc_envio_facturas_cabeceras2 SET archivofactura='`+archivo+`' where id=`+id+` `);
        }
    }
}

exports.generarTgr = async (req, resp) => {
    var sche_generarPdfFacturas = require('node-schedule');

    sche_generarPdfFacturas.scheduleJob('*/45 * * * * *', () => {
        console.log("");
        console.log("DESCARGANDO TGR");
        generarTgr();

    });

    async function generarTgr()
    {

        const request = require('request');

        //const img_dir = 'C:/Users/Gabriel/Desktop/WSC/maximise_schedule/public/tgr/imagenes/';
        //const save_dir = 'C:/Users/Gabriel/Desktop/WSC/BACKEND/public/files/tgr/';

        const img_dir = 'C:/Users/Administrator/Documents/maximise_schedule/public/tgr/imagenes/';
        const save_dir = 'C:/Users/Administrator/Documents/wscargo/restserver/public/files/tgr/';

        var pendiente = await client.query(` SELECT * FROM public.queue_tgr WHERE estado='PENDIENTE' ORDER BY id ASC limit 1 `);

        if(pendiente.rows.length>0)
        {

            const id = pendiente.rows[0].id;

            const rut = pendiente.rows[0].rut.split('-')[0];
            const dv = pendiente.rows[0].rut.split('-')[1];
            const folio = pendiente.rows[0].folio.split('-')[0];

            update_estado(id, 'PROCESANDO');


            const data = {
                rut: rut,
                dv: dv,
                formulario: '15',
                folio: folio,
                button: 'Enviar'
            };

            const options = {
                url: 'https://www.tesoreria.cl/portal/comprobantePago/goListaPagos.do',
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain',
                    'Content-Length': data.length
                },
                json: true,
                form: {
                    rut: rut,
                    dv: dv,
                    formulario: '15',
                    folio: folio,
                    button: 'Enviar'
                }
            };

            request.post(options, (err, res, body) => {

                if(err)
                {
                    console.log(" --------------------------- ERROR REPORT ");
                    console.log(" --------------------------- ERROR REPORT "+JSON.stringify(err));
                    console.log(" --------------------------- ERROR REPORT ");
                    console.log("  ");
                    console.log("  ");
                    console.log("  ");
                    update_estado(id, 'ERROR GET');

                }
                else if(body)
                {

                    let aux_body = body.replace('/web/Contenido/ImagenesSitio/logos/logo_teso1.png', '{{logo_teso1}}');
                    aux_body = aux_body.replace('/web/Contenido/ImagenesSitio/TimbreInternetPagado.jpg', '{{TimbreInternetPagado}}');
                    aux_body = aux_body.replace('/web/Contenido/ImagenesSitio/Firma_JefeOperaciones.jpg', '{{Firma_JefeOperaciones}}');

                    const regex = /<td id=(")bcTarget(")><input type=(")hidden(") value=(")\w+(") id=(")codigoBarra(")/;
                    const re_linea = /\d+/g;

                    const id_codigo = aux_body.match(regex)[0].match(re_linea)[0];

                    /**         SE CREA QR           **/
                    const JsBarcode = require('jsbarcode');

                    var { createCanvas } = require("canvas");
                    var canvas = createCanvas();

                    JsBarcode(canvas, id_codigo, {
                        width:2,
                        height:80,
                        fontSize: 12,
                        font: 'Arial'
                    });

                    try {
                        fs.writeFileSync(img_dir + 'barcode.png', canvas.toBuffer())

                    } catch (err) {
                        console.log(err)
                        update_estado(id, 'ERROR BARCODE');

                    }


                    /**             SE INCLUYE EN HTML Y SE GENERA IMAGEN               **/

                    let html_codigo = `
                            <td align="center">
                                <img src="{{barcode}}" alt="barcode" >
                            </td>
                        </tr>
                        <tr>
                    `;


                    let aux_body2 = aux_body.split(/<td id="bcTarget"><input type="hidden" value="[\s\S\n\r^]*<\/td>[\s\S\n\r^]*<tr>/g).join(html_codigo);

                    let aux_body3 = aux_body2.split('Google Tag Manager')

                    let aux_body4 = aux_body3[0] + aux_body3[2] + aux_body3[4];

                    const nodeHtmlToImage = require('node-html-to-image')

                    const image = fs.readFileSync(img_dir + 'barcode.png');
                    const base64Image = new Buffer.from(image).toString('base64');
                    const dataURI = 'data:image/jpeg;base64,' + base64Image

                    const image2 = fs.readFileSync(img_dir + 'logo_teso1.png');
                    const base64Image2 = new Buffer.from(image2).toString('base64');
                    const dataURI2 = 'data:image2/jpeg;base64,' + base64Image2

                    const image3 = fs.readFileSync(img_dir + 'TimbreInternetPagado.jpg');
                    const base64Image3 = new Buffer.from(image3).toString('base64');
                    const dataURI3 = 'data:image3/jpeg;base64,' + base64Image3

                    const image4 = fs.readFileSync(img_dir + 'Firma_JefeOperaciones.jpg');
                    const base64Image4 = new Buffer.from(image4).toString('base64');
                    const dataURI4 = 'data:image4/jpeg;base64,' + base64Image4

                    nodeHtmlToImage({
                        output: save_dir + 'TGR' + folio.substring(folio.length-5) + '.jpg',
                        html: aux_body4,
                        content: { barcode: dataURI, logo_teso1: dataURI2, TimbreInternetPagado: dataURI3, Firma_JefeOperaciones: dataURI4 },
                    })

                    fs.unlinkSync(img_dir + 'barcode.png');
                    update_estado(id, 'DESCARGADO');
                }

            });

        } else {
            console.log("NO QUEDAN TGR POR GENERAR");

        }

        async function update_estado(id, estado)
        {
            console.log(` UPDATE public.queue_tgr SET estado='`+estado+`' where id=`+id+` `);
            await client.query(` UPDATE public.queue_tgr SET estado='`+estado+`' where id=`+id+` `);
        }
    }
}
