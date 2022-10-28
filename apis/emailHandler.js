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
        copia_oculta
    }=objEmail;
      let sql = {
        text:`INSERT INTO public.email_envios_logs(
            estado, desde, para, respondera, asunto, fecha, texto, nombre, enlace, comercial, adjunto, destino, tipo, email_comercial, datos_adicionales, datos, tipo_id, copia, copia_oculta)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) RETURNING*`,
        values:['PENDIENTE','',para,respondera,asunto,fecha,texto,nombre,enlace,comercial,adjunto,null,tipo,email_comercial,datos_adicionales,datos,tipo_id,copia,copia_oculta]
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