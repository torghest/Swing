/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package swing;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import oracle.jdbc.driver.OracleTypes;

/**
 *
 * @author alumno
 */
public class AccesoJDBC {
    
    private Connection conn;
    
    public AccesoJDBC(){
        try{
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            this.conn = DriverManager.getConnection
            ("jdbc:oracle:thin:@localhost:1521:XE","system","javaoracle");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    
    public ResultSet mosDept(){
        ResultSet rs = null;
        try{
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery("Select dept_no, dnombre, loc from dept order by dept_no");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rs;
    }
    
    public  String[] getApellidos(){
        String[] res;
        
        try{
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("Select apellido from plantilla");
            String aux = "";
            int num = 0;
            while (rs.next()){
                aux += rs.getString("apellido").trim().replaceAll(" ", "//") + " ";
                num++;
            }
            res = new String[num];
            StringTokenizer st = new StringTokenizer(aux.trim(), " ");
            for (int i = 0; i < num; i++){
                res[i] = st.nextToken().replaceAll("//", " ");
            }            
        } catch (Exception e) {
            e.printStackTrace();
            res = new String[1];
            res[0] = "Sin datos";
        }
        return res;
    }
    
    public ResultSet getApellidosCursor(){
        ResultSet rs = null;
        try{
            CallableStatement cs = conn.prepareCall("{? = call DevolverCursor.obtenerEmpleados}");
            cs.registerOutParameter(1, OracleTypes.CURSOR);
            cs.execute();
            rs = (ResultSet) cs.getObject(1);
        } catch (Exception e){
            e.printStackTrace();
        }
        return rs;
    }
    
    public  int[] getDepts(){
        int[] res;
        
        try{
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("Select dept_no from dept");
            String aux = "";
            int num = 0;
            while (rs.next()){
                aux += rs.getString("dept_no") + " ";
                num++;
            }
            res = new int[num];
            StringTokenizer st = new StringTokenizer(aux.trim(), " ");
            for (int i = 0; i < num; i++){
                res[i] = Integer.parseInt(st.nextToken());
            }            
        } catch (Exception e) {
            e.printStackTrace();
            res = new int[1];
            res[0] = 0;
        }
        return res;
    }
    
    public double[] consDeptSal(int dept_no){
        double[] res = new double[3];
        try{
            CallableStatement cs = conn.prepareCall("{CALL DEPT_SAL(?,?,?,?)}");
            cs.setInt(1, dept_no);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.registerOutParameter(3, Types.INTEGER);
            cs.registerOutParameter(4, Types.DOUBLE);
            cs.executeQuery();
            res [0] = cs.getInt(2);
            res [1] = cs.getInt(3);
            res [2] = cs.getDouble(4);
        } catch (Exception e){
            e.printStackTrace();
            res [0] = 0;
            res [1] = 0;
            res [2] = 0;
        }
        return res;
    }
    
    public String selFuncion(String apellido){
        String res;
        try{
            PreparedStatement ps = conn.prepareStatement
                ("SELECT funcion FROM plantilla WHERE apellido = ?");
            ps.setString(1, apellido);
            ResultSet rs = ps.executeQuery();
            if (rs.next())
                res = rs.getString("funcion");
            else
                res = "Sin datos";
        } catch (Exception e){
            e.printStackTrace();
            res = "Sin datos";
        }        
        return res;
    }
    
    public void updFuncion(String apellido, String funcion){
        try{
            CallableStatement cs = conn.prepareCall("{call CAMBIAR_FUNCION(?,?)}");
            cs.setString(1, apellido);
            cs.setString(2, funcion);
            cs.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public int altaEmpleado(int emp_no, String apellido, String oficio, int dir,
            String fx_alta, int salario, int comision, int dept_no){
        int res = 0;
        try{
            CallableStatement cs = conn.prepareCall("{call ALTA_EMPLEADO(?,?,?,?,to_date(?,'dd-mm-yyyy'),?,?,?)}");
            cs.setInt(1, emp_no);
            cs.setString(2, apellido);
            cs.setString(3, oficio);
            cs.setInt(4, dir);
            cs.setString(5, fx_alta);
            cs.setInt(6, salario);
            cs.setInt(7, comision);
            cs.setInt(8, dept_no);

            res = cs.executeUpdate();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }
    
    public String[] consEmpleado(String emp_no){
        String[] res;
        res = new String[3];
        
        try{
            Statement stmt = conn.createStatement();
            ResultSet resul = stmt.executeQuery("Select apellido, salario, nvl(comision,0) from emp where emp_no = " + emp_no);
            if (resul.next())
                for (int i = 0; i < 3; i++)
                    res[i] = resul.getString(i+1);
            else
                for (int i = 0; i < 3; i++)
                    res[i] = "............";
        } catch (Exception e) {
            e.printStackTrace();
        }

        return res;
    }
    
    public ResultSet consEmpApe(String apellido){
        ResultSet rs = null;
        try{
            PreparedStatement stmt = conn.prepareCall("Select oficio,salario,nvl(comision,0) comision from emp where apellido = ?");
            stmt.setString(1, apellido);
            rs = stmt.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rs;
    }
    
    public int insDept(int dept_no, String dNombre, String loc){
        int res;
        try {
            PreparedStatement pSTMT = conn.prepareStatement
            ("INSERT INTO dept VALUES (?,?,?)");
            pSTMT.setInt(1, dept_no);
            pSTMT.setString(2, dNombre);
            pSTMT.setString(3, loc);
            res = pSTMT.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
            res = 0;
        }
        return res;
    }
    
    public String[] getOficios(){
        String[] res;
        try{
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT DISTINCT oficio FROM emp");
            String aux = "";
            int num = 0;
            while (rs.next()){
                aux += rs.getString("oficio") + " ";
                num++;
            }
            res = new String[num];
            StringTokenizer st = new StringTokenizer(aux.trim(), " ");
            for (int i = 0; i < num; i++){
                res[i] = st.nextToken();
            }            
        } catch (Exception e) {
            e.printStackTrace();
            res = new String[1];
            res[0] = "Sin datos";
        }
        return res;
    }
    
    public String[] selPorOficio(String oficio){
        String[] res;
        try{
            PreparedStatement ps = conn.prepareStatement
                ("SELECT emp_no,apellido,oficio,nvl(dir,0),fecha_alt,salario,"
                        + "nvl(comision,0),dept_no FROM emp WHERE oficio = ?");
            ps.setString(1, oficio);
            ResultSet rs = ps.executeQuery();
            String aux = "";
            int num = 0;
            while (rs.next()){
                for (int i = 1; i <= 7; i++){
                    aux += rs.getString(i).replaceAll("00:00:00.0", "").trim()+"\t";
                }
                aux += rs.getString(8) + " ";
                num++;
            }
            res = new String[num];
            StringTokenizer st = new StringTokenizer(aux.trim(), " ");
            for (int i = 0; i < num; i++){
                res[i] = st.nextToken();
            }            
        } catch (Exception e){
            e.printStackTrace();
            res = new String[1];
            res[0] = "Sin datos";
        }        
        return res;
    }
    
    public String[] consNSS(int nss){
        String[] res = new String[2];
        try{
            CallableStatement cs = conn.prepareCall("{call MOSTRAR_NSS(?,?,?)}");
            cs.setInt(1, nss);
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.registerOutParameter(3, Types.VARCHAR);
            cs.execute();
            res[0] = cs.getString(2);
            res[1] = cs.getString(3);
        } catch (Exception e){
            e.printStackTrace();
            res[0] = "............";
            res[1] = res[0];
        }
        return res;
    }
    
    public int delDoctor(String apellido){
        int res;
        try{
            CallableStatement cs = conn.prepareCall("{call PK_DOCTOR.ELIMINAR(?,?)}");
            cs.setString(1, apellido);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            res = cs.getInt(2);
        } catch (Exception e){
            e.printStackTrace();
            res = 0;
        }
        return res;
    }
    
    
}
