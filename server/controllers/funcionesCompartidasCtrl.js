const enviarEmail = require('../../handlers/email');
const client = require('../config/db.client');
const moment=require('moment');
const QRCode = require("qrcode");
const PDF = require('pdfkit');//Importando la libreria de PDFkit
const fs = require('fs');
const { verify } = require('crypto');
const path = require('path');
const axios = require("axios");
const { json } = require('body-parser');
const e = require('express');

/**
* @desc Obtiene el ultimo ejecutivo vigente por cliente
* @param Cliente
* @return informacion ejecutivo
*/
exports.get_comercial_vigente_data = async (Cliente) => {
    
    return await client.query(`
    SELECT 
    usu.rut,
    usu.usuario,
    usu.nombre,
    usu.apellidos,
    usu.email,
    usu.telefono
    FROM public.usuario as usu
    inner join public.clientes as cli on 
    case when cli.estado_consolidado='SI' and cli.fk_ejecutivocuenta is not null then usu.id=cli.fk_ejecutivocuenta
    else usu.id=cli.fk_comercial end
    where 
    cli.id=`+Cliente+`
    `);
}

/**
* @desc Obtiene el ultimo ejecutivo vigente por cliente
* @param Cliente
* @return informacion ejecutivo
*/
exports.get_comercial_vigente = async (Cliente) => {
    
    return await client.query(`
    SELECT 
    concat(usu.nombre,' ',usu.apellidos) as nombre
    ,usu.email
    ,usu.telefono 
    FROM public.usuario as usu
    inner join public.clientes as cli on 
    case when cli.estado_consolidado='SI' and cli.fk_ejecutivocuenta is not null then usu.id=cli.fk_ejecutivocuenta
    else usu.id=cli.fk_comercial end
    where 
    cli.id=`+Cliente+`
    `);
}

exports.FirmaDigital_EnviarEnrolamiento = async (fk_cliente) => {

    var Condicion = `
    where
    coalesce(cli.firma_digital_estado,'')!='OK'
    and coalesce(cli.firma_digital_estado,'')!='PENDIENTE'
    and coalesce(cli.firma_digital_estado,'')!='ERROR GRUPO'
    and coalesce(cli.firma_digital_estado,'')!='ERROR MANDATO'
    `;

    if(fk_cliente==null)
    {
        Condicion += ` and to_date(sla.fecha_de_creacion_del_consolidado, 'YYYY/MM/DD') >= now()::date - INTERVAL '12 MONTH' `;
    }
    else
    {
        Condicion += ` and cli.id=`+fk_cliente+` `;
    }

    /* ELIMINAR */
    Condicion = `
    where
    (
    coalesce(cli.firma_digital_estado,'')!='OK'
    and coalesce(cli.firma_digital_estado,'')!='PENDIENTE'
    and coalesce(cli.firma_digital_estado,'')!='ERROR GRUPO'
    and coalesce(cli.firma_digital_estado,'')!='ERROR MANDATO'
    and cli.id=2500
    )
    `;
    /* ELIMINAR */

    var InfoCliente = await client.query(`
    SELECT
    cli.id as id_ws
    , coalesce(cli.codigo,'') as nombre_corto
    , coalesce(cli.rut,'') as rut
    , coalesce(cli."razonSocial",'') as cliente
    , cli.telefono1
    , cli."dteEmail"
    , coalesce(rep.nombre,'') as rep_nombre
    , coalesce(rep.apellido,'') as rep_apellidos
    , coalesce(rep.rut,'') as rep_rut
    , coalesce(replace(rep.email,' ',''),'') as rep_email
    , coalesce(rep.firma_digital_estado,'') as rep_firma_estado
    , coalesce(rep.firma_digital_id,'') as rep_firma_id
    , count(sla.id_consolidado_comercial) as cant_consolidados
    , cli.firma_digital_estado
    , cli.firma_digital_grupo
    , cli.firma_digital_mandato
    , cli.firma_digital_vigencia

    , concat(dir.direccion,' Nº ',dir.numero,', ',com.nombre,', ',reg.nombre) as direccion
    FROM
    public.clientes as cli

    inner join public.clientes_contactos as rep on rep.id=(SELECT
    temp1.id
    from public.clientes_contactos as temp1
    where
    temp1.fk_cliente=cli.id
    and LENGTH(TRIM(temp1.nombre))>5
	and LENGTH(TRIM(temp1.email))>5
    and temp1.fk_tipo=4
    and temp1.estado is true order by temp1.id desc limit 1)

    inner join public.sla_00_completo as sla on cli.id::text=sla.id_cliente

    inner join public.clientes_direcciones as dir on dir.id=(SELECT
    temp1.id
    from public.clientes_direcciones as temp1
    where temp1.fk_tipo=1 and temp1.fk_cliente=cli.id and temp1.estado=0 order by temp1.id desc limit 1)
    inner join public.comunas as com on dir.fk_comuna=com.id
    inner join public.region as reg on dir.fk_region=reg.id

    `+Condicion+`

    group by
    cli.id
    , coalesce(cli.codigo,'')
    , coalesce(cli.rut,'')
    , coalesce(cli."razonSocial",'')
    , cli.telefono1
    , cli."dteEmail"
    , coalesce(rep.nombre,'')
    , coalesce(rep.apellido,'')
    , coalesce(rep.rut,'')
    , coalesce(replace(rep.email,' ',''),'')
    , coalesce(rep.firma_digital_estado,'')
    , coalesce(rep.firma_digital_id,'')
    , concat(dir.direccion,' Nº ',dir.numero,', ',com.nombre,', ',reg.nombre)
    , cli.firma_digital_estado
    , cli.firma_digital_grupo
    , cli.firma_digital_mandato
    , cli.firma_digital_vigencia

    order by
    cli.id
    asc
    limit 1
    `);

    if(InfoCliente && InfoCliente.rows && InfoCliente.rows.length>0)
    {
        const fs = require("fs");
        const pdf = require('html-pdf');

        let rep_nombre = InfoCliente.rows[0].rep_nombre.toUpperCase()+' '+InfoCliente.rows[0].rep_apellidos.toUpperCase().trim().trim().trim();
        let rep_rut = InfoCliente.rows[0].rep_rut.toUpperCase().trim().trim().trim();
        let rep_email = InfoCliente.rows[0].rep_email.toLowerCase().trim().trim().trim();
        let rep_firma_estado = InfoCliente.rows[0].rep_firma_estado.toLowerCase().trim().trim().trim();
        let cliente = InfoCliente.rows[0].cliente.toUpperCase().trim().trim().trim();
        let direccion = InfoCliente.rows[0].direccion.toUpperCase().trim().trim().trim();
        let cliente_nombre_corto = InfoCliente.rows[0].nombre_corto.toUpperCase().trim().trim().trim();

        let cli_id = InfoCliente.rows[0].id_ws;

        if( rep_nombre.length>5 && rep_rut.length>=8 && rep_email.length>5 && cliente.length>=1 && direccion.length>=5)
        {
            var fecha =  new Date();
            var moment = require('moment'); moment.locale('es');
            var mandato = 'files/mandatos/'+InfoCliente.rows[0].id_ws+''+moment(fecha).format('YYYYMMDDHHmmss')+'.pdf';
            fecha = moment(fecha).format('LL').toUpperCase();

            const content = `
            <!DOCTYPE html>
            <html>
            <head>
            <title>MANDATO</title>
            </head>
            <style>
                .page{
                    size: 21cm 29.7cm;
                    margin: 10mm 10mm 10mm 10mm;
                }
                @media print {
                .break-page {
                    page-break-after: always;
                }
            }
            </style>
            <body class="page">

                <p style="text-align:center">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <u>
                                <span style="font-size:12.0pt">
                                    <span style="font-family:&quot;Garamond&quot;,serif">
                                        MANDATO PARA AGENTE DE ADUANAS
                                    </span>
                                </span>
                            </u>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">&nbsp;</p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    Por medios digitales, con fecha <strong>`+fecha+`</strong>, en este acto suscribe el presente documento, don <strong>`+rep_nombre+`</strong>, cédula de identidad Nº <strong>`+rep_rut+`</strong>, correo electrónico <strong>`+rep_email+`</strong>, en representación legal de <strong>`+cliente+`</strong>, según se acreditará, ambos con domicilio en <strong>`+direccion+`</strong>, (en adelante el &ldquo;<u>MANDANTE</u>&rdquo;), quien expone:
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">PRIMERO</span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>CONSTITUCIÓN DE MANDATO.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    Que, por medio de este acto, el MANDANTE viene en conferir un mandato (en adelante el &ldquo;<u>MANDATO</u>&rdquo;) en los t&eacute;rminos establecidos en el art&iacute;culo 197 del Decreto con Fuerza de Ley N&deg;30 que aprueba el texto refundido, coordinado y sistematizado del decreto con fuerza de ley de hacienda N&ordm; 213, de 1953, sobre ordenanza de aduanas, pero tan amplio y bastante como en derecho se requiere en favor de don <strong>Hernan Adolfo Soto Ulloa</strong>, agente de aduanas, c&eacute;dula de identidad n&uacute;mero 9.264.772-2 (en adelante el &ldquo;<u>MANDATARIO</u>&rdquo;), para que en su nombre y representaci&oacute;n se encargue del despacho de sus mercanc&iacute;as en el ingreso al territorio de la Rep&uacute;blica de Chile. 
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">
                                            SEGUNDO
                                        </span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>FACULTADES DEL MANDATARIO.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    En ejercicio de su encargo el MANDATARIO estar&aacute; premunido de las siguientes facultades:
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <ol style="list-style-type:lower-alpha">
                    <li style="text-align:justify">
                        <span style="font-size:11pt">
                            <span style="font-family:Calibri,sans-serif">
                                <span style="font-size:12.0pt"><span style="font-family:&quot;Garamond&quot;,serif">
                                    Retirar las mercanc&iacute;as de la potestad aduanera.
                                </span>
                            </span>
                        </span>
                    </span>
                </li>
                    <li style="text-align:justify">
                        <span style="font-size:11pt">
                            <span style="font-family:Calibri,sans-serif">
                                <span style="font-size:12.0pt"><span style="font-family:&quot;Garamond&quot;,serif">
                                    Formular peticiones y reclamaciones ante la Autoridad Aduanera.
                                </span>
                            </span>
                        </span>
                    </span>
                </li>
                    <li style="text-align:justify">
                        <span style="font-size:11pt">
                            <span style="font-family:Calibri,sans-serif">
                                <span style="font-size:12.0pt">
                                    <span style="font-family:&quot;Garamond&quot;,serif">
                                        Solicitar y percibir por v&iacute;a administrativa devoluciones de dineros o cualquier otra que sea consecuencia del despacho.
                                    </span>
                                </span>
                            </span>
                        </span>
                    </li>
                    <li style="text-align:justify">
                        <span style="font-size:11pt">
                            <span style="font-family:Calibri,sans-serif">
                                <span style="font-size:12.0pt">
                                    <span style="font-family:&quot;Garamond&quot;,serif">
                                        En general, realizar todos los actos o tr&aacute;mites relacionados directamente con el despacho encargado.
                                    </span>
                                </span>
                            </span>
                        </span>
                    </li>
                </ol>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">
                                            TERCERO
                                        </span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>VIGENCIA DEL MANDATO.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    El presente mandato tendr&aacute; una duraci&oacute;n de 2 a&ntilde;os contados desde la fecha en que se suscribe este instrumento y se entiende conferido para todos los despachos que ocurran en dicho per&iacute;odo.
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">
                                            CUARTO
                                        </span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>VALIDEZ LEGAL.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    Se otorga el presente mandato de conformidad a la Resoluci&oacute;n Exenta N&deg; 2299 de fecha 30 de septiembre de 2021, modificada por Resoluci&oacute;n Exenta N&deg; 2416 de fecha 13 de octubre de 2021, ambas del Sr. Director Nacional de Aduanas.
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">
                                            QUINTO
                                        </span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>FIRMA.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    Se suscribe este documento con firma electr&oacute;nica avanzada por el MANDANTE, la cual es provista por la entidad acreditadora BPO-Advisors (IDok) Acreditada seg&uacute;n&nbsp;R.A.E. N&ordm; 3696, de fecha 6 de&nbsp;noviembre de 2017, de la Subsecretar&iacute;a de Econom&iacute;a y Empresas de Menor Tama&ntilde;o.
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    La validez de la firma que se inserta a este documento pueden verificarse en: 
                                </span>
                            </span>
                            <a href="https://firmaya.idok.cl/doc_validator" style="color:#0563c1; text-decoration:underline">
                                <span style="font-size:12.0pt">
                                    <span style="font-family:&quot;Garamond&quot;,serif">
                                        https://firmaya.idok.cl/doc_validator
                                    </span>
                                </span>
                            </a>
                        </span>
                    </span>
                </p>

            </body>
            </html>
            `;

            var options = { "height": "380mm",
                "width": "286mm",
            };

            pdf.create(content, options).toFile('C:/Users/Administrator/Documents/wscargo/restserver/public/'+mandato, async function(err, res)
            {
                if (err)
                {
                    await client.query(` UPDATE public.clientes SET firma_digital_estado='ERROR MANDATO', firma_digital_mandato=null WHERE id=`+cli_id+` `);
                }
                else
                {
                    const newRut = rep_rut.replace(/\./g,'').replace(/\-/g, '').trim().toLowerCase();
                    const lastDigit = newRut.substr(-1, 1);
                    const rutDigit = newRut.substr(0, newRut.length-1)
                    let format = '';
                    for (let i = rutDigit.length; i > 0; i--)
                    {
                        const e = rutDigit.charAt(i-1);
                        format = e.concat(format);
                        if (i % 3 === 0)
                        {
                            format = '.'.concat(format);
                        }
                    }

                    rep_rut = format.concat('-').concat(lastDigit).replace('.', '').replace('.', '').replace('.', '').replace('.', '').trim().toUpperCase();

                    var grupo_nombre = 'TEST - '+cli_id+' '+cliente_nombre_corto;
                    var documento_nombre = 'TEST - '+'MANDATO_'+cli_id+'_'+cliente_nombre_corto;

                    let PdfBuff = fs.readFileSync('./public/'+mandato);
                    let PdfBuff64 = PdfBuff.toString('base64');

                    var FirmaYaData = {
                        group_name: grupo_nombre,
                        document_name: documento_nombre ,
                        members: [{ rut:rep_rut, email:rep_email },],
                        url_redirect: 'www.google.cl',
                        notify_users: true,
                        pdf: PdfBuff64
                    };
                    const FirmaYaConfig = {
                        headers: {
                            'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjp7IiRvaWQiOiI2MmNjMmEwMmVkZjQxNTc1NmM4ZmM2ZTUifX0.NWvyrYfkYWRbzlrXTAWHRvwhRt4BqIVGsGk4mg7QSCc',
                            'Content-Type': 'application/json',
                        },
                    };
                    var RequestAxios = await axios.post('https://firmaya.idok.cl/api/corp/groups/create', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                    console.log(".......");
                    console.log(".......");
                    console.log(" DETALLE RESPUESTA "+JSON.stringify(RequestAxios.data));

                    if( RequestAxios.status=='200' )
                    {
                        await client.query(`
                        UPDATE
                        public.clientes
                        SET
                        firma_digital_estado='PENDIENTE'
                        , firma_digital_grupo='`+RequestAxios.data.id+`'
                        , firma_digital_mandato='`+mandato+`'
                        , firma_digital_tipo='FIRMA YA'
                        WHERE
                        id=`+cli_id+`
                        `);

                        if( RequestAxios.data.members.length>0 )
                        {
                            for( var i=0; i<RequestAxios.data.members.length; i++)
                            {
                                if(RequestAxios.data.members[i].status.toUpperCase()=='ENROLADO')
                                {
                                    var AuxRutRep = RequestAxios.data.members[0].rut.replace('.', '').replace('.', '').replace('.', '').replace('.', '').replace('-', '').replace('-', '').replace('-', '').trim().toUpperCase();
                                    await client.query(`
                                    UPDATE
                                    public.clientes_contactos
                                    SET
                                    firma_digital_estado='OK'
                                    , firma_digital_id='`+RequestAxios.data.members[0].id+`'
                                    , firma_digital_vigencia=''
                                    WHERE
                                    REPLACE(REPLACE(REPLACE(REPLACE(UPPER(rut),'.',''),'.',''),'.',''),'-','')='`+AuxRutRep+`'
                                    `);
                                }
                            }
                        }

                        /*
                        enviarEmail.mail_envio_invitacion_enrolamiento({
                            asunto:"WS CARGO - Invitación de enrolamiento firma digital " + moment().format('DD-MM-YYYY HH:mm:ss')
                            ,fecha:moment().format('DD-MM-YYYY')
                            ,email:rep_email
                            ,nombre:rep_nombre
                        });
                        */
                    }
                    else
                    {

                    }
                }
            });
        }
        else
        {
            await client.query(` UPDATE public.clientes SET firma_digital_estado='ERROR MANDATO', firma_digital_mandato=null WHERE id=`+cli_id+` `);
        }
    }
}

exports.FirmaDigital_ConsultarEstado = async (fk_cliente) => {

    var fecha =  new Date();
    var moment = require('moment'); moment.locale('es');
    fecha = moment(fecha).format('DD-MM-YYYY');

    const FirmaYaConfig = {
        headers: {
            'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjp7IiRvaWQiOiI2MmNjMmEwMmVkZjQxNTc1NmM4ZmM2ZTUifX0.NWvyrYfkYWRbzlrXTAWHRvwhRt4BqIVGsGk4mg7QSCc',
            'Content-Type': 'application/json',
        },
    };

    var FirmaYaData = { };
    console.log("FECHA ACTIVIDAD "+fecha);
    console.log("FECHA ACTIVIDAD "+fecha);
    console.log("FECHA ACTIVIDAD "+fecha);
    console.log("FECHA ACTIVIDAD "+fecha);
    console.log(".......");
    console.log(".......");
    console.log(".......");

    if(fk_cliente!=null)
    {
        var Condicion = `
        where
        ( coalesce(cli.firma_digital_estado,'')='PENDIENTE' and cli.firma_digital_actividad is null and cli.id=`+fk_cliente+` )
        or ( coalesce(cli.firma_digital_estado,'')='PENDIENTE' and cli.id=`+fk_cliente+` and cli.firma_digital_actividad <= now()::date - INTERVAL '2 day')
        `;
    }
    else
    {
        var Condicion = `
        where
        ( coalesce(cli.firma_digital_estado,'')='PENDIENTE' and cli.firma_digital_actividad is null )
        or ( coalesce(cli.firma_digital_estado,'')='PENDIENTE' and cli.firma_digital_actividad <= now()::date - INTERVAL '2 day')
        `;
    }

    /* ELIMINAR */
    Condicion = `
    where
    cli.id=2500
    `;
    /* ELIMINAR */
    var InfoCliente = await client.query(`
    SELECT
    cli.id as id_ws
    , coalesce(cli.firma_digital_estado,'') as firma_digital_estado
    , coalesce(cli.firma_digital_grupo,'') as firma_digital_grupo
    , coalesce(cli.firma_digital_mandato,'') as firma_digital_mandato
    , cli.firma_digital_vigencia

    FROM
    public.clientes as cli

    `+Condicion+`

    order by
    cli.id
    asc
    limit 1
    `);

    if(InfoCliente && InfoCliente.rows && InfoCliente.rows.length>0)
    {
        const fs = require("fs");

        let cli_id      = InfoCliente.rows[0].id_ws;
        let cli_grupo   = InfoCliente.rows[0].firma_digital_grupo;
        console.log(".::.");
        console.log(".::.");
        console.log(".::.");
        console.log("Consultando grupo "+cli_grupo);
        if( cli_grupo.length>=5 )
        {
            try{
                FirmaYaData = {
                    id: cli_grupo,
                };

                var RequestAxiosGrupo = await axios.post('https://firmaya.idok.cl/api/corp/groups/show', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                console.log(" DETALLE RESPUESTA CONSULTA GRUPO "+JSON.stringify(RequestAxiosGrupo.data));
            }
            catch (err)
            {
                console.log(".::.");
                console.log(".::.");
                console.log(".::.");
                console.log("ERROR CONSULTA GRUPO "+JSON.stringify(err));
                await client.query(`
                UPDATE
                public.clientes
                SET
                firma_digital_estado='ERROR GRUPO'
                , firma_digital_actividad='`+fecha+`'
                WHERE
                id=`+cli_id+`
                `);
            }

            if( typeof RequestAxiosGrupo !== 'undefined' )
            {
                if( RequestAxiosGrupo.status=='200' )
                {
                    if( RequestAxiosGrupo.data.signs_allowed!=true )
                    {
                        console.log("ID "+cli_grupo);
                        FirmaYaData = { 
                            id: cli_grupo,
                            notify_users: false,
                        };

                        await axios.post('https://firmaya.idok.cl/api/corp/groups/turn_on_signs', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                    }

                    if( RequestAxiosGrupo.data.documents.length>0 )
                    {
                        for( var i=0; i<RequestAxiosGrupo.data.documents.length; i++)
                        {
                            console.log(".......");
                            console.log(".......");
                            console.log(".......");
                            console.log(" CONSULTANDO CICLO "+i);
                            var ExisteDocumento = await client.query(`
                            SELECT
                            *
                            FROM
                            public.clientes_mandatos
                            WHERE
                            fk_cliente=`+cli_id+`
                            and
                            mandato_id='`+RequestAxiosGrupo.data.documents[i].id+`'
                            and
                            coalesce(mandato_estado,'')!='OK'
                            `);

                            if ( ExisteDocumento.rows.length<=0)
                            {
                                await client.query(` 
                                INSERT INTO
                                public.clientes_mandatos (fk_cliente, mandato_id, mandato_estado, mandato_vigencia, mandato_ruta, mandato_actividad, mandato_tipo)
                                values (`+cli_id+`, '`+RequestAxiosGrupo.data.documents[i].id+`', 'PENDIENTE', '', '', '`+fecha+`', 'FIRMA YA')
                                `);
                            }

                            ExisteDocumento = await client.query(`
                            SELECT
                            *
                            FROM
                            public.clientes_mandatos
                            WHERE
                            fk_cliente=`+cli_id+`
                            and
                            mandato_id='`+RequestAxiosGrupo.data.documents[i].id+`'
                            and
                            coalesce(mandato_estado,'')!='OK'
                            `);

                            if( ExisteDocumento.rows[0].mandato_estado=='PENDIENTE' )
                            {
                                FirmaYaData = {
                                    mxml_id: RequestAxiosGrupo.data.documents[i].id,
                                };

                                var RequestAxiosDocumento = await axios.post('https://firmaya.idok.cl/api/corp/groups/document_status', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                                console.log(".......");
                                console.log(".......");
                                console.log(".......");
                                console.log(" DETALLE RESPUESTA CONSULTA ESTADO DOCUMENTO "+JSON.stringify(RequestAxiosDocumento.data));

                                if( RequestAxiosDocumento.data.signers.length>0)
                                {
                                    await client.query(`
                                    UPDATE
                                    public.clientes_mandatos
                                    SET
                                    mandato_actividad='`+fecha+`',
                                    mandato_estado='OK',
                                    mandato_firmante_id='`+RequestAxiosDocumento.data.signers[0].id+`',
                                    mandato_firmante_email='`+RequestAxiosDocumento.data.signers[0].email+`'
                                    WHERE
                                    mandato_id='`+RequestAxiosGrupo.data.documents[i].id+`'
                                    `);

                                    await client.query(`
                                    UPDATE
                                    public.clientes
                                    SET
                                    firma_digital_actividad='`+fecha+`',
                                    firma_digital_estado='OK'
                                    WHERE
                                    mandato_id='`+RequestAxiosGrupo.data.documents[i].id+`'
                                    `);
                                }
                                else
                                {
                                    await client.query(`
                                    UPDATE
                                    public.clientes_mandatos
                                    SET
                                    mandato_actividad='`+fecha+`'
                                    WHERE
                                    mandato_id='`+RequestAxiosGrupo.data.documents[i].id+`'
                                    `);
                                }
                            }
                        }
                    }

                }
                else
                {
                    await client.query(`
                    UPDATE
                    public.clientes
                    SET
                    firma_digital_estado='ERROR GRUPO'
                    , firma_digital_actividad='`+fecha+`'
                    WHERE
                    id=`+cli_id+`
                    `);
                }
            }
        }
        else
        {
            await client.query(`
            UPDATE
            public.clientes
            SET
            firma_digital_actividad='`+fecha+`'
            WHERE
            id=`+cli_id+`
            `);
        }
    }
}

