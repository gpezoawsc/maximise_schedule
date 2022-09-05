const nodemailer = require('nodemailer');
const juice = require('juice');
const htmltoText = require('html-to-text');
const util = require('util');
const pug = require('pug');
const path = require('path');
const emailConfig = require('../config/emails');
const { POINT_CONVERSION_HYBRID } = require('constants');
const console = require('console');
/*

let transport_TNM = nodemailer.createTransport({
    host: 'smtp.mailtrap.io',
    port: '25',
    auth: { user: '24846bcaa0c0bb', pass: 'db13cc9b4ab980'},
    secureConnection: false,
    tls: { ciphers: 'SSLv3' }   
});*/


let transport_TNM = nodemailer.createTransport(emailConfig.transport_TNM);


/**********************************************/
/**********************************************/
/**********************************************/

const view_mail_nuevo_usuario = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_nuevo_usuario.pug', opciones);
    return juice(html);
}

exports.mail_nuevo_usuario = async(opciones) => {
    const html = view_mail_nuevo_usuario(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        /*to: opciones.email,*/
        to: 'eduardo.vidal@wscargo.cl',
        cc: 'eduardo.vidal@wscargo.cl',
        subject: opciones.asunto,
        text,
        html
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NUEVO USUARIO OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NUEVO USUARIO ERROR "+err);
        return false;
    });
    return estado;
}

/**********************************************/
/**********************************************/
/**********************************************/

const view_mail_notificacion_tarifa = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_tarifa.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_tarifa = async(opciones) => {
    const html = view_mail_notificacion_tarifa(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        /*to: opciones.email,*/
        to: 'eduardo.vidal@wscargo.cl',
        cc: 'eduardo.vidal@wscargo.cl',
        subject: opciones.asunto,
        text,
        html
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NUEVA TARIFA OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NUEVA TARIFA ERROR "+err);
        return false;
    });
    return estado;
}

/**********************************************/
/**********************************************/
/**********************************************/

const view_mail_notificacion_planificacion_confirmada = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_planificacion_confirmada.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_planificacion_confirmada = async(opciones) => {
    const html = view_mail_notificacion_planificacion_confirmada(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.email,
        //cc: 'pbarria.reyes@gmail.com',
        subject: opciones.asunto,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO CONFIRMACION PLANFICACION OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO CONFIRMACION PLANFICACION ERROR "+err);
        return false;
    });
    return estado;
}

const view_notificacion_etiqueta_cliente = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_etiqueta_cliente.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_etiqueta_cliente = async(opciones) => {
    const html = view_notificacion_etiqueta_cliente(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.email,
        //cc: 'pbarria.reyes@gmail.com',
        subject: opciones.asunto,
        text,
        html,
        attachments: [
        {
            filename: 'etiqueta-wsc-'+opciones.fk_cliente+'.pdf', // <= Here: made sure file name match
            path: path.join('C:/Users/Administrator/Documents/wscargo/restserver/server/controllers/etiquetas/'+opciones.fk_cliente+'.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO ETIQUETA CLIENTE OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO ETIQUETA CLIENTE ERROR "+err);
        return false;
    });
    return estado;
}

/*
*   
*
*/
const view_mail_notificacion_proceso_documental = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_proceso_documental.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_proceso_documental = async(opciones) => {
    const html = view_mail_notificacion_proceso_documental(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.to,
        cc: opciones.cc,
        subject: opciones.asunto,
        text,
        html,
        attachments: opciones.attachment 
        
        /*[
            {
                filename: opciones.attachment[0].filename, // <= Here: made sure file name match
                path: opciones.attachment[0].path, // <= Here
                contentType: opciones.attachment[0].contentType
            },
            {
                filename: opciones.attachment[1].filename, // <= Here: made sure file name match
                path: opciones.attachment[1].path, // <= Here
                contentType: opciones.attachment[1].contentType
            }
        ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NOTIFICACION PROCESO DOCUMENTAL OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION PROCESO DOCUMENTAL ERROR "+err);
        return false;
    });
    return estado;
}


const view_mail_notificacion_recepcion = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_recepcion_new.pug', opciones);
    return juice(html);
}


exports.mail_notificacion_recepcion = async(opciones) => {

    console.log("CONFIG CORREO "+JSON.stringify(emailConfig));
    console.log("CONFIG CORREO "+JSON.stringify(emailConfig));
    console.log("CONFIG CORREO "+JSON.stringify(emailConfig));
    const html = view_mail_notificacion_recepcion(opciones);
    const text = htmltoText.fromString(html);
    let email1='wscargo@wscargo.cl';
   /* /*
    if(opciones.emailcomercial && opciones.emailcomercial.length>0){
        email1=opciones.emailcomercial;
    }*/
    let opcionesEmailWsc = {
        from: email1,
        to:'wscargo@wscargo.cl',
        bcc: opciones.email,
        cliente: opciones.cliente,
        replyTo: opciones.emailcomercial,
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO CONFIRMACION RECEPCION OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO CONFIRMACION RECEPCION ERROR "+err);
        return false;
    });
    
    return estado;
}

const view_mail_notificacion_contenedor_proforma = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_contenedor_proforma.pug', opciones);
    return juice(html);
}
exports.mail_notificacion_contenedor_proforma = async(opciones) => {
    const html = view_mail_notificacion_contenedor_proforma(opciones);
    const text = htmltoText.fromString(html);

    const sendMail=async(opcionesEmail)=>{
        var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log("info",info)
        console.log(" ENVIO CORREO NOTIFICACION PROFORMA OK ");
            return true;
        }).catch(function(err){
            console.log(" ENVIO CORREO NOTIFICACION PROFORMA ERROR "+err);
            return false;
        });
    };
    
    if(opciones.emails && opciones.emails.length>0){

        for(let i=0;i< opciones.emails.length;i++){
            let opcionesEmail = {
                from: 'wscargo@wscargo.cl',
                to: 'wscargo@wscargo.cl',
                replyTo:opciones.emails[i].emailComercial,
                bcc:opciones.emails[i].emailCliente+';'+opciones.emails[i].emailComercial,
                fecha:opciones.fecha,
                subject: opciones.asunto+' (cliente '+opciones.emails[i].fk_cliente+')',
                text,
                html
            };

            await sendMail(opcionesEmail);
        }
        
        return true;
    }else{
        return false;
    }
}



const view_mail_notificacion_question = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_question.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_question = async (opciones) => {
    const html = view_mail_notificacion_question(opciones);
    const text = htmltoText.fromString(html);

    let remitente='wscargo@wscargo.cl';
    /*if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        remitente=opciones.comercial.email;
    }*/

    let opcionesEmail = {
      from: remitente,
      to: opciones.to,
      //cc: opciones.cc,
      subject: opciones.asunto,
      text,
      html,
      //attachments: opciones.attachment
    };
    var estado = await transport_TNM.sendMail(opcionesEmail).then(function (info) {
        //console.log("info",info);
        console.log(" ENVIO CORREO QUESTION OK ");
        return true;
      })
      .catch(function (err) {
        console.log(" ENVIO CORREO QUESTION ERROR " + err);
        return false;
      });
}

/**********************************************/
/**********************************************/

const view_mail_nota_de_cobro = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_nota_de_cobro.pug', opciones);
    return juice(html);
}

exports.mail_nota_de_cobro = async(opciones) => {
    const html = view_mail_nota_de_cobro(opciones);
    const text = htmltoText.fromString(html);

    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.destinatario,
        cc: opciones.copia_destinatario,
        replyTo: opciones.copia_destinatario,
        subject: opciones.asunto,
        text,
        html,
        attachments: [
            {
                filename: opciones.pdf_file_name,
                path: opciones.pdf_file
            },
        ],
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NOTA DE COBRO OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTA DE COBRO ERROR "+err);
        return false;
    });
    return estado;
}

/**********************************************/
/**********************************************/

const view_mail_etiquetas_2022_clientes = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_etiquetas_2022.pug', opciones);
    return juice(html);
}

exports.mail_etiquetas_2022_clientes = async(opciones) => {
    const html = view_mail_etiquetas_2022_clientes(opciones);
    const text = htmltoText.fromString(html);

    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.to,
        //to: 'Eduardo.Vidal@wscargo.cl',
        cc: opciones.cc,
        //cc: 'ariel.aguilar@wscargo.cl',
        replyTo: opciones.emailEjecutivo,
        //replyTo: 'ariel.aguilar@wscargo.cl',
        subject: opciones.subject,
        text,
        html,
        attachments: opciones.attachments
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO ETIQUETAS 2022 OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO  ETIQUETAS 2022 ERROR "+err);
        return false;
    });
    return estado;
}

const view_mail_notificacion_exp_digital = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_exp_digital.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_exp_digital = async(opciones) => {
    const html = view_mail_notificacion_exp_digital(opciones);
    const text = htmltoText.fromString(html);
    let opcionesEmailWsc = {
        from: 'wscargo@wscargo.cl',
        to:'wscargo@wscargo.cl',
        bcc: opciones.email,
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL ERROR "+err);
        return false;
    });
    
    return estado;
}


const view_mail_notificacion_1 = (opciones) => {
    const html = pug.renderFile('./views/emails/notificaciones/notif_1.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_1 = async(opciones) => {
    const html = view_mail_notificacion_1(opciones);
    const text = htmltoText.fromString(html);
    let remitente='wscargo@wscargo.cl';
   /* if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        remitente=opciones.comercial.email;
    }*/
    let opcionesEmailWsc = {
        from: remitente,
        to:opciones.email,
        bcc:'wscargo@wscargo.cl',
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        opcionesEmailWsc.replyTo=opciones.comercial.email;
    }

    if(opciones.attachments){console.log('ADJUNTANDO ARCHIVO');
        if(opciones.attachments.tipo==11 && opciones.attachments.carpeta!=null && opciones.attachments.carpeta.length>0){//nota cobro
            let carpt=opciones.attachments.carpeta.slice(0,-2);
            opcionesEmailWsc.attachments=[
                {
                    filename: 'nota_cobro_'+opciones.attachments.carpeta+'.pdf', // <= Here: made sure file name match
                    path: path.join('C:/Users/Administrator/Documents/wscargo/restserver/public/files/notas_de_cobro/'+carpt+'/'+opciones.attachments.carpeta+'.pdf'), // <= Here
                    contentType: 'application/pdf'
                }
            ]
        }else if(opciones.attachments.tipo==12 && opciones.attachments.carpeta!=null && opciones.attachments.carpeta.length>0){//DIN
            let carpt=opciones.attachments.carpeta.slice(0,-2);
            opcionesEmailWsc.attachments=[
                {
                    filename: 'fact_iva_'+opciones.attachments.carpeta+'.pdf', // <= Here: made sure file name match
                    path: path.join('C:/Users/Administrator/Documents/wscargo/restserver/public/files/cotizaciones/cotizacion_'+opciones.attachments.carpeta+'.pdf'), // <= Here
                    contentType: 'application/pdf'
                }
            ]
        }else if(opciones.attachments.tipo==13 && opciones.attachments.carpeta!=null && opciones.attachments.carpeta.length>0){//FACTURA
            let carpt=opciones.attachments.carpeta.slice(0,-2);
            opcionesEmailWsc.attachments=[
                {
                    filename: 'factura_'+opciones.attachments.carpeta+'.pdf', // <= Here: made sure file name match
                    path: path.join('C:/Users/Administrator/Documents/wscargo/restserver/public/files/fact_cargarfacturas/'+opciones.attachments.carpeta+'.pdf'), // <= Here
                    contentType: 'application/pdf'
                }
            ]
        }
    }

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL ERROR "+err);
        return false;
    });
    
    return estado;
}

const view_mail_notificacion_6 = (opciones) => {
    const html = pug.renderFile('./views/emails/notificaciones/notif_6.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_6 = async(opciones) => {
    const html = view_mail_notificacion_6(opciones);
    const text = htmltoText.fromString(html);
    let remitente='wscargo@wscargo.cl';
    /*if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        remitente=opciones.comercial.email;
    }*/
    let opcionesEmailWsc = {
        from: remitente,
        to:opciones.email,
        bcc:'wscargo@wscargo.cl',
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        opcionesEmailWsc.replyTo=opciones.comercial.email;
    }

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL ERROR "+err);
        return false;
    });
    
    return estado;
}