const client = require('../server/config/db.client');
const insertEmailQueue = async (objEmail) => {
    try {
    const{
        para,
        asunto,
        fecha,
        texto,
        nombre,
        enlace,
        comercial,
        adjunto,
        tipo,
        respondera,
        email_comercial,
        datos_adicionales,
        datos,
        tipo_id,
        copia,
        copia_oculta,
        para_respaldo,
        copia_respaldo,
        copia_oculta_respaldo,
        fecha_hora
    }=objEmail;
      let sql = {
        text:`INSERT INTO public.email_envios_logs(
            estado, desde, para, respondera, asunto, fecha, texto, nombre, enlace, comercial, adjunto, destino, tipo, email_comercial, datos_adicionales, datos, tipo_id, copia, copia_oculta, para_respaldo, copia_respaldo, copia_oculta_respaldo, fecha_hora)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23) RETURNING*`,
        values:['PENDIENTE','',para,respondera,asunto,fecha,texto,nombre,enlace,comercial,adjunto,null,tipo,email_comercial,datos_adicionales,datos,tipo_id,copia,copia_oculta,para_respaldo,copia_respaldo,copia_oculta_respaldo,fecha_hora]
      };

      let result = await client.query(sql);
      
      return result.rows[0];

    } catch (e) {
      console.log("Error en insertEmailQueue :" + e.message);
    }
};


module.exports = { 
    insertEmailQueue
}