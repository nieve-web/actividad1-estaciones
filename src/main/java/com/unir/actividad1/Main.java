package com.unir.actividad1;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Main {
  
  // Conexión a PostgreSQL datos dentro de Docker
  private static final String URL  = "jdbc:postgresql://localhost:5432/estaciones";
  private static final String USER = "estaciones_user";
  private static final String PASS = "estaciones_pass";
  
  // Tipos carburante que vamos a cargar para la actividad
  private static final String GASOLINA_95_E5 = "Gasolina 95 E5";
  private static final String GASOLEO_A      = "Gasóleo A";
  
  public static void main(String[] args) {
    
    System.out.println("\n=== INICIO DEL PROCESO DE INGESTA ===\n");
    
    try (Connection conn = DriverManager.getConnection(URL, USER, PASS)) {
      System.out.println("Conexión establecida con PostgreSQL.");
      
      System.out.println("Limpiando tablas...");
      resetDatabase(conn);
      
      System.out.println("\nEstado inicial:");
      printCounts(conn);
      
      System.out.println("\n--> Cargando tipos de carburante...");
      createTiposCarburante(conn);
      
      System.out.println("\n--> Cargando datos de preciosEESS.csv...");
      loadPreciosEESS(conn);
      
      System.out.println("\n--> Cargando datos de embarcaciones.csv...");
      loadEmbarcaciones(conn);
      
      System.out.println("\nEstado final:");
      printCounts(conn);
      
      System.out.println("\n=== INGESTA COMPLETADA ===");
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

    /* ============================================================
       =============== 1. INSERTAR TIPOS DE CARBURANTE ============
       ============================================================ */
  
  private static void createTiposCarburante(Connection conn) throws SQLException {
    insertTipoCarburante(conn, GASOLINA_95_E5);
    insertTipoCarburante(conn, GASOLEO_A);
  }
  
  private static void insertTipoCarburante(Connection conn, String nombre) throws SQLException {
    String sql = "INSERT INTO tipo_carburante (nombre) VALUES (?) ON CONFLICT DO NOTHING";
    
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, nombre);
      ps.executeUpdate();
      System.out.println("Insertado tipo carburante: " + nombre);
    }
  }
  
  private static int getTipoCarburanteId(Connection conn, String nombre) throws SQLException {
    String sql = "SELECT id_tipo_carburante FROM tipo_carburante WHERE nombre = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, nombre);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) return rs.getInt(1);
      }
    }
    throw new SQLException("Tipo carburante no encontrado: " + nombre);
  }

   /* ============================================================
   =============== 2. CARGA DE ESTACIONES EESS ================
   ============================================================ */
  
  private static void loadPreciosEESS(Connection conn) throws Exception {
    Path path = Paths.get("src/main/resources/preciosEESS.csv");
    
    try (BufferedReader br = Files.newBufferedReader(path)) {
      
      // 1. Leer fecha desde la cabecera
      String header = br.readLine();
      LocalDateTime fecha = parseFechaCabecera(header);
      
      // 2. Saltar dos líneas irrelevantes
      br.readLine();
      br.readLine();
      
      // 3. Leer la cabecera "real" (no se usa directamente)
      br.readLine();
      
      // 4. Procesar datos
      String linea;
      while ((linea = br.readLine()) != null) {
        
        String[] c = linea.split(";", -1);
        
        String provincia     = c[0];
        String municipio     = c[1];
        String localidad     = c[2];
        String codigoPostal  = c[3];
        String direccion     = c[4];
        String margen        = c[5];
        String longitudStr   = c[6];
        String latitudStr    = c[7];
        String precio95Str   = c[9];
        String precioGasA    = c[14];
        String empresa       = c[35];
        
        double lat = parseDoubleOrNull(latitudStr);
        double lon = parseDoubleOrNull(longitudStr);
        
        if (Double.isNaN(lat) || Double.isNaN(lon)) {
          System.out.println("⚠ Saltando estación con coordenadas inválidas (EESS): " + direccion);
          continue;
        }
        
        int empresaId = insertEmpresa(conn, empresa);
        int estacionId = insertEstacion(conn, empresaId, "TERRESTRE",
            provincia, municipio, localidad, codigoPostal, direccion,
            margen, latitudStr, longitudStr, null);
        
        if (!precio95Str.isBlank())
          insertPrecio(conn, estacionId, GASOLINA_95_E5, precio95Str, fecha);
        
        if (!precioGasA.isBlank())
          insertPrecio(conn, estacionId, GASOLEO_A, precioGasA, fecha);
      }
    }
  }

/* ============================================================
   ============== 3. CARGA DE ESTACIONES MARÍTIMAS ============
   ============================================================ */
  
  private static void loadEmbarcaciones(Connection conn) throws Exception {
    Path path = Paths.get("src/main/resources/embarcaciones.csv");
    
    try (BufferedReader br = Files.newBufferedReader(path)) {
      
      String header = br.readLine();
      LocalDateTime fecha = parseFechaCabecera(header);
      
      // Saltar las tres líneas siguientes
      br.readLine();
      br.readLine();
      br.readLine(); // cabecera real
      
      String linea;
      while ((linea = br.readLine()) != null) {
        
        String[] c = linea.split(";", -1);
        
        String provincia     = c[0];
        String municipio     = c[1];
        String localidad     = c[3];
        String codigoPostal  = c[4];
        String direccion     = c[5];
        String longitudStr   = c[6];
        String latitudStr    = c[7];
        String precio95Str   = c[8];
        String precioGasAStr = c[10];
        String empresa       = c[22];
        
        double lat = parseDoubleOrNull(latitudStr);
        double lon = parseDoubleOrNull(longitudStr);
        
        if (Double.isNaN(lat) || Double.isNaN(lon)) {
          System.out.println("⚠ Saltando estación con coordenadas inválidas (MARITIMA): " + direccion);
          continue;
        }
        
        int empresaId = insertEmpresa(conn, empresa);
        int estacionId = insertEstacion(conn, empresaId, "MARITIMA",
            provincia, municipio, localidad, codigoPostal, direccion,
            null, latitudStr, longitudStr, null);
        
        if (!precio95Str.isBlank())
          insertPrecio(conn, estacionId, GASOLINA_95_E5, precio95Str, fecha);
        
        if (!precioGasAStr.isBlank())
          insertPrecio(conn, estacionId, GASOLEO_A, precioGasAStr, fecha);
      }
    }
  }
  
  private static double parseDoubleOrNull(String value) {
    if (value == null || value.isBlank()) return Double.NaN;
    value = value.trim().replace(",", ".");
    try {
      return Double.parseDouble(value);
    } catch (Exception e) {
      return Double.NaN;
    }
  }

      /* ============================================================
       =============== MÉTODOS DE INSERCIÓN =======================
       ============================================================ */
  
  private static int insertEmpresa(Connection conn, String nombre) throws SQLException {
    String sql =
        "INSERT INTO empresa (nombre) VALUES (?) ON CONFLICT DO NOTHING RETURNING id_empresa";
    
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, nombre);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) return rs.getInt(1);
      }
    }
    
    // Si ya existía, obtener su ID
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT id_empresa FROM empresa WHERE nombre = ?")) {
      ps.setString(1, nombre);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) return rs.getInt(1);
      }
    }
    throw new SQLException("Error insertando empresa: " + nombre);
  }
  
  private static int insertEstacion(Connection conn, int empresaId, String tipo,
                                    String provincia, String municipio, String localidad,
                                    String cp, String direccion, String margen,
                                    String latStr, String lonStr, String horario)
      throws SQLException {
    
    String sql = """
                INSERT INTO estacion_servicio
                (id_empresa, tipo_estacion, provincia, municipio, localidad, codigo_postal,
                 direccion, margen, latitud, longitud, horario)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id_estacion
                """;
    
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setInt(1, empresaId);
      ps.setString(2, tipo);
      ps.setString(3, provincia);
      ps.setString(4, municipio);
      ps.setString(5, localidad);
      ps.setString(6, cp);
      ps.setString(7, direccion);
      ps.setString(8, margen);
      ps.setDouble(9, Double.parseDouble(latStr.replace(",", ".")));
      ps.setDouble(10, Double.parseDouble(lonStr.replace(",", ".")));
      ps.setString(11, horario);
      
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) return rs.getInt(1);
      }
    }
    
    throw new SQLException("Error insertando estación.");
  }
  
  private static void insertPrecio(Connection conn, int estacionId, String tipo, String precioStr,
                                   LocalDateTime fecha) throws SQLException {
    
    int tipoId = getTipoCarburanteId(conn, tipo);
    
    String sql = """
                INSERT INTO precio_carburante(id_estacion, id_tipo_carburante, fecha, precio)
                VALUES (?, ?, ?, ?)
                """;
    
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setInt(1, estacionId);
      ps.setInt(2, tipoId);
      ps.setTimestamp(3, Timestamp.valueOf(fecha));
      ps.setDouble(4, Double.parseDouble(precioStr.replace(",", ".")));
      ps.executeUpdate();
    }
  }
  
  private static LocalDateTime parseFechaCabecera(String linea) {
    String fechaStr = linea.split(";")[1].trim();
    DateTimeFormatter f = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");
    return LocalDateTime.parse(fechaStr, f);
  }

    /* ============================================================
       =================== MÉTODOS DE LOG ==========================
       ============================================================ */
  
  private static void printCounts(Connection conn) throws SQLException {
    printCount(conn, "empresa");
    printCount(conn, "estacion_servicio");
    printCount(conn, "tipo_carburante");
    printCount(conn, "precio_carburante");
  }
  
  private static void printCount(Connection conn, String table) throws SQLException {
    String sql = "SELECT COUNT(*) FROM " + table;
    try (PreparedStatement ps = conn.prepareStatement(sql);
         ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        System.out.printf("Tabla %-20s → %d registros%n", table, rs.getLong(1));
      }
    }
  }
  
  private static void resetDatabase(Connection conn) throws SQLException {
    String sql = """
        TRUNCATE TABLE precio_carburante, estacion_servicio, empresa, tipo_carburante
        RESTART IDENTITY CASCADE;
    """;
    
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.executeUpdate();
    }
    
    System.out.println("Tablas limpiadas correctamente.");
  }
}
