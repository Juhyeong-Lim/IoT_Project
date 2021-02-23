package IoT_Project;

import java.io.*;
import java.sql.*;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;


public class DBConnection {
	private Connection con = null;
	private PreparedStatement pstmt = null;
	private ResultSet rs = null;
	
	public DBConnection() {
		try {
			Context context = new InitialContext();
			DataSource dataSource = (DataSource)context.lookup("java:comp/env/jdbc/iotDB");
			con = dataSource.getConnection();
			
		} catch (NamingException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/* device 테이블 insert 쿼리 */
	public int insertDevice(String macAddr) {
		int id = -1;
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		String query = "insert into device(mac_addr, reg_date) values (?, ?)";

		try {
			pstmt = con.prepareStatement(query); 
			pstmt.setString(1, macAddr);
			pstmt.setTimestamp(2, timestamp);
			pstmt.executeUpdate();
			
			// 등록 후 디바이스 id 반환
			query = "select id from device where mac_addr = ?";
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, macAddr);
			rs = pstmt.executeQuery();
			
			if(rs.next()) {
				id = rs.getInt("id");
				System.out.println("Device ID: " + id);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			
		} finally {
			try {
				if(con != null) {
					con.close();
				}
				
				if (pstmt != null) {
					pstmt.close();
				}
				
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return id;
	}
	
	/* status 테이블 insert 쿼리 */
	public void insertStatus(int deviceId, String action, int sensorData) {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis()); // 임시
		// 디바이스 id, 열림/닫힘정보, 센서데이터, 감지시간
		String query = "insert into status(device_id, action, sensor_data, time) values(?, ?, ?, ?)";
		
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, deviceId);
			pstmt.setString(2, action);
			pstmt.setInt(3, sensorData);
			pstmt.setTimestamp(4, timestamp); // 임시
			pstmt.executeUpdate();
			
		} catch(SQLException e) {
			e.printStackTrace();
			
		} finally {
			try {
				if(con != null) {
					con.close();
				}
				
				if (pstmt != null) {
					pstmt.close();
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/* device 테이블 위치 수정 쿼리 */
	public void update(int deviceId, String position) {
		String query = "update device set position=? where id=?";
		
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, position);
			pstmt.setInt(2, deviceId);
			pstmt.executeUpdate(); 
			
		} catch (SQLException e) {
			e.printStackTrace();
			
		} finally {
			try {
				if(con != null) {
					con.close();
				}
				
				if (pstmt != null) {
					pstmt.close();
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void deleteDevice(int deviceId) {
		String query = "delete from device where id=?";
		
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, deviceId);
			pstmt.executeUpdate(); 
			
		} catch (SQLException e) {
			e.printStackTrace();
			
		} finally {
			try {
				if(con != null) {
					con.close();
				}
				
				if (pstmt != null) {
					pstmt.close();
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void deleteStatus(int deviceId) {
		String query = "delete from status where device_id=?";
		
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, deviceId);
			pstmt.executeUpdate(); 
			
		} catch (SQLException e) {
			e.printStackTrace();
			
		} finally {
			try {
				if(con != null) {
					con.close();
				}
				
				if (pstmt != null) {
					pstmt.close();
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Connection getConnection() {
		return con;
	}
}
