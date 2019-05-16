package com.informacolombia.cargabalances.persistencia;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import com.informacolombia.cargabalances.entidades.InfColCargaBalances;
import com.informacolombia.cargabalances.exceptions.InfColManejoExcepciones;
import com.informacolombia.cargabalances.interfaces.IInfColEntidad;
import com.informacolombia.cargabalances.interfaces.InfColVariablesGenerales;
import com.informacolombia.cargabalances.persistencia.InfColDaoConsultas;
import com.informacolombia.cargabalances.utils.DynamicClass;
import com.informacolombia.cargabalances.utils.InfColCargaProperties;
import com.informacolombia.cargabalances.utils.InfColUtilidades;

/**
 * Se crean los metodos que ejecutan las consulta SQL
 * 
 * @author ebarrero
 *
 */

public class InfColDaoEjecutarConsultas {

	private String query;
	private String mensajeError = "";
	private String codigoFuente;
	private Connection conexion;
	private String usuario = "CARGABALANCESV2";
	private InfColUtilidades utilidades = new InfColUtilidades();
	private InfColDaoConsultas getquery = new InfColDaoConsultas();
	private InfColManejoExcepciones excepcion = new InfColManejoExcepciones(InfColDaoEjecutarConsultas.class);
	private HashMap<String, Method> metodosGet = new HashMap<String, Method>();
	private HashMap<String, Method> metodosSet = new HashMap<String, Method>();
	private InfColCargaProperties cargarProperties = new InfColCargaProperties();
	private HashMap<String, String> propInfoBasica;
	private HashMap<String, String> propPartidasCargar;
	private DynamicClass dynamicClass = new DynamicClass();

	public InfColDaoEjecutarConsultas() {

	}

	public InfColDaoEjecutarConsultas(HashMap<String, String> hashParam) {
		this.codigoFuente = hashParam.get("--codfuente");
		Method[] metodosEntidad = InfColCargaBalances.class.getMethods();
		metodosGet = utilidades.hashMapMetodos(metodosEntidad, "get");
		metodosSet = utilidades.hashMapMetodos(metodosEntidad, "set");
	}

	/**
	 * Metodo que ejecuta una consulta y retorna un ResultSet
	 * 
	 * @return ResultSet
	 */
	public ResultSet consultaCamarasComercio() {
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			conexion = InfColDaoConexion.getConexion();
			query = getquery.consultarCamaras;
			ps = conexion.prepareStatement(query);
			rs = ps.executeQuery();
		} catch (SQLException e) {
			excepcion.logMensajeExcepcion(e, "Error en el metodo: consultaCamarasComercio() ");
			System.exit(1);
		} finally {
			try {
				ps.close();
				InfColDaoConexion.cerrarConexion();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
		return rs;
	}

	/**
	 * Consulta en la db el ici para cada entidad
	 * 
	 * @param nit
	 * @return String
	 */
	public ArrayList<IInfColEntidad> reconciliarIci(ArrayList<IInfColEntidad> listaEntidadesFabricadas) {
		ArrayList<IInfColEntidad> entidadReconciliada = new ArrayList<IInfColEntidad>();
		excepcion.logInfo("Reconciliando ici y extrayendo fecha Costitucion");
		try {
			conexion = InfColDaoConexion.getConexion();
			for (IInfColEntidad entidades : listaEntidadesFabricadas) {
				String ici, parametro = null;
				LocalDate fechaConstitucion;
				InfColCargaBalances entidad = (InfColCargaBalances) entidades;
				if (codigoFuente.equalsIgnoreCase("DB")) {
					parametro = entidad.getIci();
					query = getquery.consultarFechaConstitucion;
				} else {
					parametro = calcularDigitoVerificacion(entidad.getNit());
					query = getquery.consultaFechaCostitucionYici;
				}
				if (parametro != null) {
					ResultSet rs;
					PreparedStatement ps;
					ps = conexion.prepareStatement(query);
					ps.setString(1, parametro);
					rs = ps.executeQuery();
					while (rs.next()) {
						if (codigoFuente.equalsIgnoreCase("DB")) {
							fechaConstitucion = rs.getDate("FECHA_CONSTITUCION") != null
									? rs.getDate("FECHA_CONSTITUCION").toLocalDate()
									: null;
							if (fechaConstitucion == null) {
								concatenarErrores("Fecha de constitucion vacia, para el ICI : " + parametro);
							} else {
								entidad.setFec_constitucion(fechaConstitucion);
							}
						} else {

							ici = rs.getString("ICI");
							fechaConstitucion = rs.getDate("FECHA_CONSTITUCION") != null
									? rs.getDate("FECHA_CONSTITUCION").toLocalDate()
									: null;
							if (ici == null) {
								concatenarErrores("No reconcilia ICI para el NIT : " + parametro);
							} else {
								entidad.setIci(ici);
							}
							if (fechaConstitucion == null) {
								concatenarErrores("Fecha de constitucion vacia para el NIT : " + parametro);
							} else {
								entidad.setFec_constitucion(fechaConstitucion);
							}
						}
					}
					ps.close();
					rs.close();
				} else {
					concatenarErrores("La entidad no tiene NIT o ICI");
				}
				entidad.setMensajeError(entidad.getMensajeError() + mensajeError);
				entidadReconciliada.add(entidad);
				mensajeError = "";
			}

			InfColDaoConexion.cerrarConexion();
		} catch (Exception e) {
			excepcion.logMensajeExcepcion(e, "Error en el metodo: consultarIci() ");
		}
		excepcion.logInfo("Finaliza reconciliando ici y extracion fecha Costitucion");
		return entidadReconciliada;
	}

	/**
	 * Metodo encargado de consultar el balance con una fecha en especifico, en caso
	 * de no encontrarlo trae el balance mas reciente.
	 * 
	 * @author jquevedo
	 * @param listaEntidadesFabricadas : balances a procesar
	 * @return HashMap<String, InfColCargaBalances> balanceIris
	 * 
	 */

	public HashMap<String, InfColCargaBalances> traerBalanceIris(ArrayList<IInfColEntidad> listaEntidadesFabricadas) {

		propInfoBasica = cargarProperties.cargarProperties("BalanceIris");
		HashMap<String, InfColCargaBalances> listaBalanceIris = new HashMap<String, InfColCargaBalances>();
		excepcion.logInfo("Consultado Balance IRIS");
		conexion = InfColDaoConexion.getConexion();
		for (IInfColEntidad entidades : listaEntidadesFabricadas) {

			PreparedStatement ps = null;
			PreparedStatement ps1 = null;
			ResultSet rs = null;
			PreparedStatement psPartidas = null;
			ResultSet rs1 = null;
			InfColCargaBalances entidad = (InfColCargaBalances) entidades;
			if (entidad.getFecha_cierre() != null) {
				InfColCargaBalances entidadIris = new InfColCargaBalances();
				try {
					ps = conexion.prepareStatement(getquery.consultaBalanceOldXICIFecCierre);
					psPartidas = conexion.prepareStatement(getquery.consultaPartidasXIDBalance);
					ps.setString(1, entidad.getIci());
					ps.setString(2, String.valueOf(entidad.getFecha_cierre().getYear()));
					rs = ps.executeQuery();
					if (rs.next()) {
						entidadIris = cargarEntidad(entidadIris, rs);
						psPartidas.setString(1, entidadIris.getPartBlncId());
						rs = psPartidas.executeQuery();
						entidadIris = mapearPartidas(entidadIris, rs);
						ps.close();
						rs.close();
						psPartidas.close();

					} else {
						ps.close();
						rs.close();
						ps1 = conexion.prepareStatement(getquery.consultaBalanceOldXICI);
						ps1.setString(1, entidad.getIci());
						rs1 = ps1.executeQuery();
						if (rs1.next()) {
							entidadIris = cargarEntidad(entidadIris, rs1);
							psPartidas.setString(1, entidadIris.getPartBlncId());
							rs1 = psPartidas.executeQuery();
							entidadIris = mapearPartidas(entidadIris, rs1);
							ps1.close();
							rs1.close();
							psPartidas.close();
						} else {
							ps1.close();
							rs1.close();
						}
					}
					listaBalanceIris.put(entidadIris.getIci(), entidadIris);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						ps.close();
						rs.close();
						ps1.close();
						rs.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}

				}
			}
		}
		InfColDaoConexion.cerrarConexion();
		return listaBalanceIris;

	}

	/**
	 * Metodo encargado de traer las partidas del balance en iris, se utiliza
	 * reflection y un properties BalancesIris.properties para traer los campos
	 * 
	 * @author jquevedo
	 * @param entidadIris
	 * @param             rs: resltado de la consulta
	 * @return entIris : entidad con los campos maepados
	 */
	public InfColCargaBalances cargarEntidad(InfColCargaBalances entidadIris, ResultSet rs) {

		InfColCargaBalances entIris = new InfColCargaBalances();
		try {
			for (String key : propInfoBasica.keySet()) {
				Object valor = rs.getObject(key);
				if (valor != null) {
					Method metodo = metodosSet.get("set" + key);
					Parameter[] param = metodo.getParameters();
					if (!param[0].getType().getName().contains("String")) {
						metodo.invoke(entIris, dynamicClass.castDynamicClass(param[0].getType().getName(),
								String.valueOf(valor), null));
					} else {
						metodo.invoke(entIris, String.valueOf(valor));
					}
				}
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return entIris;

	}

	/**
	 * Metodo encargado de mapear todas las partidas a la entidad,se utlizal
	 * reflection para que sea dinamico
	 * 
	 * @author jquevedo
	 * @param balanceIris
	 * @param rs          : resultado de la consulta de partidas
	 * @return balanceIris: con las partidas mapeadas
	 */
	public InfColCargaBalances mapearPartidas(InfColCargaBalances balanceIris, ResultSet rs) {
		try {
			while (rs.next()) {
				Method metodo = metodosSet.get("set" + rs.getString("PART_COD"));
				if (metodo != null) {
					metodo.invoke(balanceIris, new BigDecimal(rs.getString("PART_VAL")));
				}
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return balanceIris;
	}

	public void guardarInformacionBasica(ArrayList<IInfColEntidad> listaBalance) {

		JdbcTemplate jdbcTemplate = new JdbcTemplate(InfColDaoConexion.getConexionOracle());
		try {
			int[] mod = jdbcTemplate.batchUpdate(getquery.insertarBalance, new BatchPreparedStatementSetter() {
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					int j = 0;
					InfColCargaBalances balanceCargar = (InfColCargaBalances) listaBalance.get(i);
					ps.setInt(1, 0);
					ps.setLong(2, Long.parseLong(balanceCargar.getPartBlncId()));
					ps.setLong(3, j++); // id de prueba
					ps.setString(4, balanceCargar.getIci());
					ps.setString(5, balanceCargar.getTipoBalance());
					ps.setString(6, balanceCargar.getCod_camara());
					ps.setString(7, balanceCargar.getFecha_cierre().toString());
					ps.setInt(8, balanceCargar.getDuracion());
					ps.setString(9, balanceCargar.getTextoAuditor());
					ps.setString(10, InfColVariablesGenerales.MONEDA);
					ps.setInt(11, balanceCargar.getUnidadCarga());
					ps.setString(12, InfColVariablesGenerales.USUARIO);
					ps.setString(13, InfColVariablesGenerales.USUARIO);
					ps.setString(14, InfColVariablesGenerales.SUB_TIPO_BALANCE);
					ps.setString(15, InfColVariablesGenerales.ESTADO_ACTUAL);
				}

				@Override
				public int getBatchSize() {
					return listaBalance.size();

				}
			});
			System.out.println("datos actualizados " + mod[0]);
			InfColDaoConexion.cerrarConexion();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ArrayList<IInfColEntidad> recuperarSecuencia(ArrayList<IInfColEntidad> listaBalance) {

		ArrayList<IInfColEntidad> listaBalancesProcesados = new ArrayList<IInfColEntidad>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		conexion = InfColDaoConexion.getConexion();
		String idSecuencia = "";
		for (IInfColEntidad entidades : listaBalance) {
			try {
				InfColCargaBalances entidad = (InfColCargaBalances) entidades;
				ps = conexion.prepareStatement(getquery.consultaSecuencia);
				rs = ps.executeQuery();
				while (rs.next()) {
					idSecuencia = rs.getString(1);
				}
				entidad.setPartBlncId(idSecuencia);
				listaBalancesProcesados.add(entidad);
			} catch (Exception e) {
				e.printStackTrace(); // valida que hacer si falla
			} finally {
				try {
					rs.close();
					ps.close();
					InfColDaoConexion.cerrarConexion();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}
		}

		return listaBalancesProcesados;
	}

	public void guardarPartidas(ArrayList<IInfColEntidad> listaBalance) {
		try {
			conexion = InfColDaoConexion.getConexion();
			PreparedStatement ps;
			ps = conexion.prepareStatement(getquery.insertarPartidas);
			int counter = 0;
			for (IInfColEntidad entidades : listaBalance) {

				InfColCargaBalances entidad = (InfColCargaBalances) entidades;
				HashMap<String, BigDecimal> part = crearPartidasCargar(entidad);

				for (String key : part.keySet()) {
					try {
						BigDecimal value = (BigDecimal) part.get(key);
						ps.setInt(1, 1);
						ps.setString(2, entidad.getPartBlncId());
						ps.setString(3, key.substring(3));
						ps.setBigDecimal(4, value);
						ps.addBatch();
					} catch (Exception e) {
						excepcion.logMensajeExcepcion(e, "Error al insertar la partida " + key + ":" + part.get(key)
								+ "Para el balance " + entidad.getIci() + " se realizara rollback");
						borrarBalance(entidad.getPartBlncId());
						borrarPartidas(entidad.getPartBlncId());
						e.printStackTrace();
					}
				}
				counter++;
				if (counter == 1000) {
					ps.executeBatch();
					counter = 0;
				}
			}

			if (counter > 0) {
				ps.executeBatch();
			}
			ps.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public HashMap<String, BigDecimal> crearPartidasCargar(InfColCargaBalances balance) throws Exception {
		HashMap<String, BigDecimal> listaPartidas = new HashMap<String, BigDecimal>();
		propPartidasCargar = cargarProperties.cargarProperties("PartidasIris");
		for (String key : metodosGet.keySet()) {
			Method method = metodosGet.get(key);
			Object valor = method.invoke(balance);
			if (propPartidasCargar.containsKey(method.getName().substring(3)) && valor != null) {
				BigDecimal valorBig = new BigDecimal(valor.toString());
				listaPartidas.put(method.getName().toUpperCase(), valorBig);
			}
		}
		return listaPartidas;
	}

	/**
	 * 
	 * 
	 * @param nit
	 * @return String
	 */
	private String calcularDigitoVerificacion(String nit) {

		nit = nit.replace("-", "");
		nit = nit.replace(".", "");
		nit = nit.replace(",", "");
		nit = nit.replace(" ", "");

		if (nit != null) {
			if (nit.length() == 9 && (nit.charAt(0) == '8' || nit.charAt(0) == '9')) {
				int digito = 0, acum = 0, residuo = 0;
				char[] nit_array = nit.toCharArray();
				int[] primos = { 3, 7, 13, 17, 19, 23, 29, 37, 41, 43, 47, 53, 39, 67, 71 };
				int max = nit_array.length;

				for (int i = 0; i < nit.length(); i++) {
					acum += Integer.parseInt(String.valueOf(nit_array[max - 1])) * primos[i];
					max--;
				}
				residuo = acum % 11;
				if (residuo > 1) {
					digito = 11 - residuo;
				} else {
					digito = residuo;
				}
				return nit + digito;
			}
		}
		return nit;
	}

	/**
	 * Metodo que consulta el salario minimo de oracle, en caso de generar error se
	 * tomara el valor del la clase estatica InfColVariablesGenerales
	 * 
	 * @return BigDecimal
	 */
	public BigDecimal ConsultarSalarioMinimo() {

		String valorSalarioMinimo = "";
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			conexion = InfColDaoConexion.getConexion();
			query = getquery.consultarSalarioMinimo;
			ps = conexion.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				valorSalarioMinimo = rs.getString("SALARIOMINIMO");
			}
			ps.close();
			rs.close();
			InfColDaoConexion.cerrarConexion();
			return utilidades.StringToBigDecimal(valorSalarioMinimo);
		} catch (Exception e) {
			/*
			 * SI GENERA ALGUN ERROR EN LA CONEXION O CONSULTA A LA BASE DE DATOS, SE
			 * TOAMARA EL VALOR DE LAS VARIABLES ESTATICAS
			 */
			excepcion.logMensajeExcepcion(e, "Error en el metodo: ConsultarSalarioMinimo() ");
			excepcion.logMensajeExcepcion(e, "se tomara el valor de la clase InfColVariablesGenerales()");
			return InfColVariablesGenerales.SALARIOMINIMO;
		} finally {
			try {
				rs.close();
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * Metodo que recibe un Id de balance para ser borrado de la tabla
	 * blnc_balances, antes de borrarlo hace la insercion en la tabla
	 * blnc_balances_log
	 * 
	 * @author jquevedo
	 * @param idBalance
	 */
	public void borrarBalance(String idBalance) {
		conexion = InfColDaoConexion.getConexion();
		ResultSet rs = null;
		PreparedStatement ps = null;
		insertarBalanceLog(idBalance, "B");
		query = getquery.borrarBalance;
		try {
			ps = conexion.prepareStatement(query);
			ps.setString(1, idBalance);
			rs = ps.executeQuery();
			ps.execute("COMMIT");
		} catch (Exception e) {
			excepcion.logMensajeExcepcion(e, "Error al borrar el balance.  ID:" + idBalance);
		} finally {
			try {
				ps.close();
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}

	public void borrarBalanceMasivo(ArrayList<IInfColEntidad> listaBalanceBorrar) {

		try {
			conexion = InfColDaoConexion.getConexion();
			String query = getquery.borrarBalance;
			PreparedStatement ps = conexion.prepareStatement(query);
			int counter = 0;
			for (IInfColEntidad balances : listaBalanceBorrar) {
				InfColCargaBalances balance = (InfColCargaBalances) balances;
				try {
					insertarBalanceLog(balance.getIdBalanceIris(), "B");
					ps.setString(1, balance.getIdBalanceIris());
					ps.addBatch();
					counter++;
					if (counter == InfColVariablesGenerales.LOTE_EMPRESAS) {
						ps.executeBatch();
						counter = 0;
					}
				} catch (Exception e) {
					excepcion.logMensajeExcepcion(e,
							"Error al borrar el balance.  ID:" + ((InfColCargaBalances) balances).getIdBalanceIris());
				}
			}
			if (counter > 0) {
				ps.executeBatch();
			}
			ps.close();
			InfColDaoConexion.cerrarConexion();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * Metodo que recibe un Id de balance para ser borrado de la tabla part_partidas
	 * 
	 * @author jquevedo
	 * @param idBalance
	 */
	public void borrarPartidas(String idBalance) {
		conexion = InfColDaoConexion.getConexion();
		ResultSet rs = null;
		PreparedStatement ps = null;
		query = getquery.borrarPartidas;
		try {
			ps = conexion.prepareStatement(query);
			ps.setString(1, idBalance);
			rs = ps.executeQuery();
			ps.execute("COMMIT");
		} catch (Exception e) {
			excepcion.logMensajeExcepcion(e, "Error al borrar las partidas.  ID:" + idBalance);
		} finally {
			try {
				ps.close();
				rs.close();
				InfColDaoConexion.cerrarConexion();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public void borrarPartidaMasivo(ArrayList<IInfColEntidad> listaBalanceBorrar) {
		try {
			conexion = InfColDaoConexion.getConexion();
			String query = getquery.borrarPartidas;
			PreparedStatement ps = conexion.prepareStatement(query);
			int counter = 0;
			for (IInfColEntidad balances : listaBalanceBorrar) {
				InfColCargaBalances balance = (InfColCargaBalances) balances;
				try {
					ps.setString(1, balance.getIdBalanceIris());
					ps.addBatch();
					counter++;
					if (counter == InfColVariablesGenerales.LOTE_EMPRESAS) {
						ps.executeBatch();
						counter = 0;
					}
				} catch (Exception e) {
					excepcion.logMensajeExcepcion(e, "Error al borrar las partidas.  ID:" + balance.getIdBalanceIris());
				}
			}
			if (counter > 0) {
				ps.executeBatch();
			}
			ps.close();
			InfColDaoConexion.cerrarConexion();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Inserta un balance borrado en la tabla de log
	 * 
	 * @author jquevedo
	 * @param         idBalance--> id del balance a borrar
	 * @param acccion --> borrar 'B', modificar 'M', i inserción
	 * @return true --> transacción exitosa false --> error al insertar
	 */
	public Boolean insertarBalanceLog(String idBalance, String accion) {
		conexion = InfColDaoConexion.getConexion();
		ResultSet rs = null;
		PreparedStatement ps = null;
		query = getquery.insertarBalanceLog;
		try {
			ps = conexion.prepareStatement(query);
			ps.setString(1, usuario);
			ps.setString(2, accion);
			ps.setString(3, idBalance);
			rs = ps.executeQuery();
			ps.execute("commit");
			return true;
		} catch (SQLException e) {
			excepcion.logMensajeExcepcion(e, "Error al insertar el balance en la tabla log ID:" + idBalance);
			return false;
		} finally {
			try {
				ps.close();
				rs.close();
				InfColDaoConexion.cerrarConexion();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Metodo que cambia el estado de un balance, ademas de modificar las variables
	 * de control(fecha ultimo cambio, usu_ult_mod)
	 * 
	 * @param idBalance
	 * @param situacion
	 * @return Boolean
	 */
	public Boolean cambiarEstadoBalance(String idBalance, int situacion) {

		conexion = InfColDaoConexion.getConexion();
		ResultSet rs = null;
		PreparedStatement ps = null;
		insertarBalanceLog(idBalance, "M");
		query = getquery.cambiarEstadoBalance;
		try {
			ps = conexion.prepareStatement(query);
			ps.setInt(1, situacion);
			ps.setString(2, usuario);
			ps.setString(3, idBalance);
			rs = ps.executeQuery();
			ps.execute("COMMIT");
			return true;
		} catch (SQLException e) {
			excepcion.logMensajeExcepcion(e, "Error al modificar el estado al Balance ID:" + idBalance);
			return false;
		} finally {
			try {
				ps.close();
				rs.close();
				InfColDaoConexion.cerrarConexion();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Metodo que cambia el estado de un balance, ademas de modificar las variables
	 * de control(fecha ultimo cambio, usu_ult_mod)
	 * 
	 * @param idBalance
	 * @param situacion
	 * @return Boolean
	 */
	public void cambiarEstadoBalanceMasivo(ArrayList<IInfColEntidad> listaBalance) {

		try {
			conexion = InfColDaoConexion.getConexion();
			String query = getquery.cambiarEstadoBalance;
			PreparedStatement ps = conexion.prepareStatement(query);
			int counter = 0;
			for (IInfColEntidad balanceInterfaz : listaBalance) {
				try {
					InfColCargaBalances balance = (InfColCargaBalances) balanceInterfaz;
					insertarBalanceLog(balance.getIdBalanceIris(), "M");
					ps.setInt(1, InfColVariablesGenerales.ESTADO_ANTIGUO);
					ps.setString(2, InfColVariablesGenerales.USUARIO);
					ps.setString(3, balance.getIdBalanceIris());
					ps.addBatch();
					counter++;
					if (counter == InfColVariablesGenerales.LOTE_EMPRESAS) {
						ps.executeBatch();
						counter = 0;
					}

				} catch (SQLException e) {
					excepcion.logMensajeExcepcion(e, "Error al modificar el estado al Balance ID:"
							+ ((InfColCargaBalances) balanceInterfaz).getIdBalanceIris());
				}
			}
			if (counter > 0) {
				ps.executeBatch();
			}
			ps.close();
			InfColDaoConexion.cerrarConexion();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param error
	 */
	private void concatenarErrores(String error) {
		mensajeError += error + " | ";
	}
}