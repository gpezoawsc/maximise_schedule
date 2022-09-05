const enviarEmail = require('../../handlers/email');
const client = require('../config/db.client');
const moment=require('moment');
const QRCode = require("qrcode");
const PDF = require('pdfkit');//Importando la libreria de PDFkit
const fs = require('fs');
const { verify } = require('crypto');
const zoho = require('../../apis/zoho_crm');
const path = require('path');
const funcionesCompartidasCtrl = require('./funcionesCompartidasCtrl.js');

exports.firma_digital_enviar_enrolamiento = async (req, res) => {
    var nodeSchedule=require('node-schedule');
    nodeSchedule.scheduleJob('*/20 * * * * *', () => {
        funcion_firma_digital_enviar_enrolamiento()
    });
  
    async function funcion_firma_digital_enviar_enrolamiento(){
      console.log('tarea programada - correo enrolamiento');
      await funcionesCompartidasCtrl.FirmaDigital_EnviarEnrolamiento();
    }
  };
  
  exports.firma_digital_consultar_estado = async (req, res) => {
    var nodeSchedule=require('node-schedule');
    nodeSchedule.scheduleJob('*/20 * * * * *', () => {
        funcion_firma_digital_consultar_estado()
    });
  
    async function funcion_firma_digital_consultar_estado(){
      console.log('tarea programada - correo enrolamiento');
      await funcionesCompartidasCtrl.FirmaDigital_ConsultarEstado();
    }
  };

exports.mail_envios = async (req, resp) => {
    console.log(".::.");
    console.log(".::.");
    console.log("CONSULTANDO CORREOS ");
    var sche_mail_envios = require('node-schedule');

    sche_mail_envios.scheduleJob('*/15 * * * * *', () => {
        mail_envios();

    });

    async function mail_envios()
    {

        var Correo = await client.query(` 
        SELECT
        *
        FROM
        public.email_envios_logs
        where
        estado='PENDIENTE'
        order by id 
        asc limit 1
        `);
        
        var email = '-.com'
        function validateEmail(email) 
        {
            var re = /\S+@\S+\.\S+/;
            return re.test(email);
        }
        console.log(".::.");
        console.log(".::.");
        console.log("VALIDANDO EMAIL "+validateEmail(email));

        console.log(".::.");
        console.log(".::.");
        console.log("CANTIDAD DE CORREOS PENDIENTES "+Correo.rows.length);
        if(Correo.rows.length>0)
        {
            var EstadoCorreo = null;

            console.log(".::.");
            console.log(".::.");
            console.log("ENVIANDO CORREO "+Correo.rows[0]['id']);
            if( Correo.rows[0]['tipo']=='mail_nuevo_usuario' )
            {
                await enviarEmail.mail_nuevo_usuario({
                    nombre:Correo.rows[0]['nombre']
                    , apellido:Correo.rows[0]['datos']
                    , email:Correo.rows[0]['para']
                    , usuario:Correo.rows[0]['datos_adicionales']
                    , telefono:Correo.rows[0]['enlace']
                    , asunto:Correo.rows[0]['asunto']
                    , contrasenia:Correo.rows[0]['texto']
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_nota_de_cobro' )
            {
                EstadoCorreo = await enviarEmail.mail_nota_de_cobro({
                    pdf_file:Correo.rows[0]['datos']
                    , pdf_file_name:Correo.rows[0]['datos_adicionales']
                    , destinatario:Correo.rows[0]['para']
                    , copia_destinatario:Correo.rows[0]['copia']
                    , asunto:Correo.rows[0]['asunto']
                    , correo_sin_din:Correo.rows[0]['tipo_id']
                    , nombre_com:Correo.rows[0]['comercial']
                });

                if(EstadoCorreo==true)
                {
                    await client.query(` `+Correo.rows[0]['texto']+` `);
                }
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_planificacion_confirmada' )
            {
                EstadoCorreo = await enviarEmail.mail_notificacion_planificacion_confirmada({ 
                    asunto:Correo.rows[0]['asunto'],
                    fecha_descarga:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    contenedor:Correo.rows[0]['datos'],
                    clientes:JSON.parse(Correo.rows[0]['datos_adicionales'])
                 });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }            
            else if( Correo.rows[0]['tipo']=='mail_notificacion_proceso_documental' )
            {
                EstadoCorreo = await enviarEmail.mail_notificacion_proceso_documental({ 
                    from: 'wscargo@wscargo.cl',
                    to: Correo.rows[0]['para'],
                    cc: Correo.rows[0]['copia'],
                    asunto: Correo.rows[0]['asunto'],
                    RepresentanteLegal: Correo.rows[0]['nombre'],
                    attachment: JSON.parse(Correo.rows[0]['adjunto'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_etiquetas_2022_clientes' )
            {
                EstadoCorreo = await enviarEmail.mail_etiquetas_2022_clientes({ 
                    from: 'wscargo@wscargo.cl',
                    to: Correo.rows[0]['para'],
                    cc: Correo.rows[0]['copia'],
                    representante:Correo.rows[0]['nombre'],
                    replyTo: Correo.rows[0]['respondera'],
                    subject: Correo.rows[0]['asunto'],
                    attachments: JSON.parse(Correo.rows[0]['adjunto'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }  
            else if( Correo.rows[0]['tipo']=='mail_notificacion_etiqueta_cliente' )
            {
                EstadoCorreo = await enviarEmail.mail_notificacion_etiqueta_cliente({ 
                    asunto:Correo.rows[0]['asunto'],
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    nombre:Correo.rows[0]['nombre'],
                    fk_cliente:Correo.rows[0]['tipo_id'],
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }     
            else if( Correo.rows[0]['tipo']=='mail_notificacion_contenedor_proforma' )
            {
                EstadoCorreo = await enviarEmail.mail_notificacion_contenedor_proforma({
                    asunto:Correo.rows[0]['asunto'],
                    texto:Correo.rows[0]['texto'],
                    fecha:Correo.rows[0]['fecha'],
                    emails:JSON.parse(Correo.rows[0]['para'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_tarifa' )
            {
                EstadoCorreo = await enviarEmail.mail_notificacion_tarifa({ 
                    asunto:Correo.rows[0]['asunto'],
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    nombre:Correo.rows[0]['nombre'],
                 });  
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }            
            else if( Correo.rows[0]['tipo']=='mail_notificacion_question' )
            {
                EstadoCorreo = await enviarEmail.mail_notificacion_question({
                    asunto:Correo.rows[0]['asunto'],
                    name:Correo.rows[0]['nombre'],
                    linksQuestion:Correo.rows[0]['enlace'],
                    to:Correo.rows[0]['para'],
                    comercial:JSON.parse(Correo.rows[0]['comercial'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_6' )
            {
                EstadoCorreo = await enviarEmail.mail_notificacion_6({
                    asunto:Correo.rows[0]['asunto'],
                    nombreUsuario:Correo.rows[0]['nombre'],
                    fecha:Correo.rows[0]['fecha'],
                    host:Correo.rows[0]['enlace'],
                    tipo:Correo.rows[0]['tipo_id'],
                    email:Correo.rows[0]['para'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }            
            else if( Correo.rows[0]['tipo']=='mail_notificacion_1' )
            {
                if( Correo.rows[0]['tipo_id']=='16' )
                {
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='17' )
                {
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='18' )
                {
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='14' )
                {
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        texto:Correo.rows[0]['texto'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='22' )
                {
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='23' )
                {
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else
                {
                    if( Correo.rows[0]['adjunto']!='' && Correo.rows[0]['adjunto']!=null )
                    {
                        EstadoCorreo = await enviarEmail.mail_notificacion_1({
                            asunto:Correo.rows[0]['asunto'],
                            nombreUsuario:Correo.rows[0]['nombre'],
                            texto:Correo.rows[0]['texto'],
                            fecha:Correo.rows[0]['fecha'],
                            email:Correo.rows[0]['para'],
                            attachments:JSON.parse(Correo.rows[0]['adjunto']),
                            comercial:JSON.parse(Correo.rows[0]['comercial']),
                        });
                    }
                    else
                    {
                        EstadoCorreo = await enviarEmail.mail_notificacion_1({
                            asunto:Correo.rows[0]['asunto'],
                            nombreUsuario:Correo.rows[0]['nombre'],
                            texto:Correo.rows[0]['texto'],
                            fecha:Correo.rows[0]['fecha'],
                            email:Correo.rows[0]['para'],
                            comercial:JSON.parse(Correo.rows[0]['comercial']),
                        });
                    }
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
            }  
            else if( Correo.rows[0]['tipo']=='mail_notificacion_recepcion' )
            {
                EstadoCorreo = await enviarEmail.mail_notificacion_recepcion({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }                        
        }
        
        async function ActualizarEstadoEnvioCorreo(id, estado)
        {
            if(EstadoCorreo==true)
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIADO' where id=`+id+` `);
            }
            else if(EstadoCorreo==false)
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ERROR' where id=`+id+` `);
            }
            else
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='INDEFINIDO' where id=`+id+` `);
            }
        }
    }
}