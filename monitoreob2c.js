const axios = require('axios')
const sql = require('mssql')
const fs = require('fs')
process.title = 'MonitoreoB2C-data - PID: ' + process.pid
/*const {
  performance,
  PerformanceObserver
} = require('perf_hooks');
*/

// config server
const config = {
	user: 'sysdbo',
	password: 'QxaAvTeG',
	server: 'mtelsqlsms2',
	database: 'SMSReportes',
	connectionTimeout: 300000,
	requestTimeout: 300000,
	options: {
		encrypt: false,
	},
	pool: {
		idleTimeoutMillis: 300000,
	},
}

const configQA = {
	user: 'sysdbo',
	password: 'QxaAvTeG',
	server: 'MTELQASQLSMS2',
	database: 'SMSReportes',
	connectionTimeout: 300000,
	requestTimeout: 300000,
	options: {
		encrypt: false,
	},
	pool: {
		idleTimeoutMillis: 300000,
	},
}

function rejectAfterDelay(ms) { 
	return new Promise((_, reject) => {
		setTimeout(reject, ms, new Error("timeout"))})
}
// update Grids every 10 seconds
async function updateData() {
	let promises = []
	let t0 = new Date()
	log('Empieza ejecucion..')
	try {
		let pool = await sql.connect(config)
		log('Crea conexion')
		let spEstatus = fs.readFileSync('./storeProcedures/sp_Estatus.txt', 'utf-8')
		promises.push(pool.request().query(`${spEstatus}`))
		log('carga estatus')
		let spMonitorProveedores = fs.readFileSync('./storeProcedures/sp_MonitorProveedores.txt', 'utf-8')
		promises.push(pool.request().query(`${spMonitorProveedores}`))
		log('carga monitorProveedores')
		//let MonitorNoMigrados = pool.request().execute('sp_MonitorNoMigrados')
		let spMonitorNoMigrados = fs.readFileSync('./storeProcedures/sp_MonitorNoMigrados.txt', 'utf-8')
		promises.push(pool.request().query(`${spMonitorNoMigrados}`))
		log('carga NoMigrados')
		promises.push(pool.request().execute('sp_graficaDistProveedor'))
		log('carga grafica dona proveedor')
		promises.push(pool.request().execute('sp_graficaproveedorcarrier'))
		log('carga grafica proveedor carrier')
		let spGraficaCliente = fs.readFileSync('./storeProcedures/sp_GraficaClienteProveedor.txt', 'utf-8')
		promises.push(pool.request().query(`${spGraficaCliente}`))
		log('carga grafica cliente proveedor')
		//let MensajesPrueba = pool.request().execute('sp_MonitorMensajesPrueba')
		let spMonitorMensajesPrueba = fs.readFileSync('./storeProcedures/sp_MonitorMensajesPrueba.txt', 'utf-8')
		promises.push(pool.request().query(`${spMonitorMensajesPrueba}`))
		log('carga mensajes prueba')
		let spMonitorUsuarios = fs.readFileSync('./storeProcedures/sp_MonitorUsuarios.txt', 'utf-8')
		promises.push(pool.request().query(`${spMonitorUsuarios}`))
		log('carga monitorUsuarios')
		let spMonitorBlaster = fs.readFileSync('./storeProcedures/sp_MonitorBlaster.txt', 'utf-8')
		promises.push(pool.request().query(`${spMonitorBlaster}`))
		log('carga monitorBlaster')
		let spMonitorWCF = fs.readFileSync('./storeProcedures/sp_MonitorWCF.txt', 'utf-8')
		promises.push(pool.request().query(`${spMonitorWCF}`))
		log('carga monitorWCF')

		let results = await Promise.allSettled(promises.map(promise => Promise.race([promise,rejectAfterDelay(180000)])))
		results.map(el=> log(el.status))
		//console.log(results[3].value.recordset)
		let FechaActualizacion = new Date()
		FechaActualizacion.setHours(FechaActualizacion.getHours() - 5)
		let Data = {
			Estatus: results[0].value.recordset[0],
			MonitorProveedores: results[1].value.recordset,
			MonitorNoMigrados: results[2].value.recordset,
			GraficaDistProveedor: results[3].value.recordset,
			GraficaProveedor: results[4].value.recordset,
			GraficaCliente: results[5].value.recordset,
			MensajesPrueba: results[6].value.recordset,
			FechaActualizacion: FechaActualizacion,
			Usuarios: results[7].value.recordset,
			MonitorBlaster: results[8].value.recordsets[0],
			EstatusBlaster: results[8].value.recordsets[1][0],
			MonitorWCF: results[9].value.recordset
		}
		
		 await axios
		.post('http://localhost:3000/', Data)
		.then((res) => {
			//console.log(res.data + ' - Data upload successful')
			log('Data upload successful')
		})
		.catch((err) => {
			//console.log(err)
			log('Data upload unsuccessful', err)
		}) 
            
	} catch (err) {
		console.log(err)
		log(`ERROR: ${JSON.stringify(err)}`)
		//log(err)
		sql.close()
		setTimeout(updateData, 10000)
	}
	sql.close()
	let t1 = new Date() - t0
	//console.log('Data loading duration: ' + t1 / 1000 + 'seg')
	log('Data loading duration: ' + t1 / 1000 + 'seg')
	/*
		JC: http://172.16.5.65:3000/
		JC Marcatel: http://10.1.62.64:3000/
		Marcatel: http://mtelservice1:3000/
		Heroku: http://monitoreob2c.herokuapp.com/
		*/
	//fs.appendFile('test.txt',JSON.stringify(Data),err=>{if(err){console.log(err)}})
	
	Data = {}
	setTimeout(updateData, 10000)
}

const getDate = () => {
	let today = new Date()
	let dd = today.getDate()

	let mm = today.getMonth() + 1
	const yyyy = today.getFullYear()
	if (dd < 10) {
		dd = `0${dd}`
	}

	if (mm < 10) {
		mm = `0${mm}`
	}
	today = `${dd}-${mm}-${yyyy}`
	return today
}

function log(info) {
	let hr = new Date()
	let fecha = getDate()
	fs.appendFileSync(`./logs/logMonitoreo.${fecha}`, `${hr} | ${info}\r\n`)
	return
}

updateData()
