package com.mycompany1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Test9 {

    private static final String HOST = "jdbc:oracle:thin:@192.168.72.129:1521/";
    private static final String DB = "remot";
    private static final String USER = "mohamed";
    private static final String PASSWORD = "mohamed";

    @SuppressWarnings("CallToPrintStackTrace")
    public static void main(String[] args) throws SQLException {
        String sqlQuery = "SELECT et.ENTITY_TYPE_ID,"
                + " et.ENTITY_CAT_ID,"
                + " act.REF_ICCID,"
                + " act.ACT_DATE,"
                + "  distE.STATE,"
                + "   distE.ENTITY_NAME as DIST_ENTITY_NAME,"
                + "   pdvE.ENTITY_ID    as PDV_ENTITY_NAME,"
                + "  c.CO_ID,"
                + " c.CONSP_AGREGATION,"
                + "  c.CO_DEACTIV_DATE,"
                + "   c.SIM_BOX_STATUS,"
                + "    c.CO_HOMOLOG_STATUS,"
                + "   prcp.PROD_COM_PID,"
                + "  pkcp.PACK_COM_PID,"
                + "  prcp.PRODUCT_ID,"
                + "   pkcp.PACK_ID,"
                + "   ocp.OFF_COM_PID,"
                + "   ocp.OFFER_ID,"
                + "    ocp.COM_CONSO_VALUE,"
                + "   ocp.COM_OFFER_MAX_THRESHOLD,"
                + "   ocp.COM_OFFER_MAX_VALUE,"
                + "   ocp.COM_OFFER_MIN_THRESHOLD,"
                + "   ocp.COM_OFFER_MIN_VALUE,"
                + "    ocp.OFF_COM_PCOMMISSION_PERIOD,"
                + "   prcp.PROD_COM_PCOM,"
                + "    prcp.PROD_COM_PPAIE,"
                + "    pkcp.PACK_COM_PCOM,"
                + "    pkcp.PACK_COM_PPAIE,"
                + "   comp.COMPUTE_ID,"
                + "   comp.COMPUTE_VERSION,"
                + "   comp.COMMISSION_SIM,"
                + "   comp.COMMISSION_SHARING,"
                + "   comp.commission_fix as COMMISSION_ADD,"
                + "   comp.COMMISSION_FIX_PDV as COMMISSION_ADD_PDV,"
                + "    comp.COMMISSION_REPAYEMENT_SIM,"
                + "    compV1.COMPUTE_ID      as V1_COMPUTE_ID,"
                + "  compV1.COMMISSION_SIM as V1_COMMISSION_SIM,"
                + "  compV1.COMMISSION_SHARING as V1_COMMISSION_SHARING,"
                + "   compV1.Commission_Fix          as V1_COMMISSION_ADD,"
                + "   compV1.COMMISSION_FIX_PDV as V1_COMMISSION_ADD_PDV,"
                + "    compV1.COMMISSION_REPAYEMENT_SIM as V1_COMMISSION_REPAYEMENT_SIM,"
                + "   nvl(cons.CONSUMPTION_M,0) as CONSO_M,"
                + "         nvl(cons.CONSUMPTION_M1,0) as CONSO_M1,"
                + "         nvl(cons.CONSUMPTION_M2,0) as CONSO_M2,"
                + "       nvl(cons.CONSUMPTION_M3,0) as CONSO_M3"
                + "  FROM ACTIVATION                 act,"
                + "       STOCK_SIM                  sm,"
                + "      DISTRIBUTION_SYSTEM_ENTITY distE,"
                + "     DISTRIBUTION_SYSTEM_ENTITY pdvE,"
                + "     PACKAGE_COM_PARAMETER      pkcp,"
                + "     PRODUCT_COM_PARAMETER      prcp,"
                + "     entity_type                et,"
                + "    OFFER_COM_PARAMETER        ocp,"
                + "  contract                   c,"
                + "   COMPUTE                    comp,"
                + "   COMPUTE                    compV1,"
                + "     CONSUPTION_AGGRAGATION     cons"
                + " WHERE act.REF_ICCID = sm.REF_ICCID"
                + "   and c.CO_ICCID = act.REF_ICCID"
                + "  and c.CO_ID = cons.CONTRACT_CO_ID(+)"
                + "   and sm.DISTRIBUTOR_DISTRIBUTOR_ID = distE.ENTITY_ID"
                + "   and sm.POINT_OF_SALE_POINT_OF_SALE_ID = pdvE.ENTITY_ID"
                + "  and ((et.ENTITY_TYPE_ID = distE.ENTITY_TYPE_ID and et.entity_cat_id = 0 and distE.State=1) or"
                + "      (et.ENTITY_TYPE_ID = pdvE.ENTITY_TYPE_ID and et.entity_cat_id = 1  and pdvE.State=1))"
                + "   and act.ACT_DATE >= to_date(:startDate, 'dd-mm-yyyy')"
                + "   and act.ACT_DATE < to_date(:stopDate, 'dd-mm-yyyy') + 1"
                + "   and et.entity_cat_id = :entityCat"
                + "   and act.OFFER_OFFER_ID = ocp.OFFER_ID"
                + "  and et.ENTITY_TYPE_ID = ocp.ENTITY_TYPE_ID"
                + "   and act.PRODUCT = prcp.PRODUCT_ID"
                + "   and et.ENTITY_TYPE_ID = prcp.ENTITY_TYPE_ID"
                + "	   and act.PACK_PACK_ID = pkcp.PACK_ID"
                + "   and et.ENTITY_TYPE_ID = pkcp.ENTITY_TYPE_ID"
                + "   and act.REF_ICCID = comp.ACTIVATION_ID(+)"
                + "   and comp.COMPUTE_VERSION(+) = 0"
                + "  and act.REF_ICCID = compV1.ACTIVATION_ID(+)"
                + "  and compV1.COMPUTE_VERSION(+) = 1"
                + "   and NOT EXISTS"
                + " (select ocpp.version"
                + "          from OFFER_COM_PARAMETER ocpp"
                + "         where ocpp.OFFER_ID = ocp.OFFER_ID"
                + "          and act.ACT_DATE >= ocpp.OFF_COM_PAPPLICATION_DATE"
                + "          and ocp.ENTITY_TYPE_ID = ocpp.ENTITY_TYPE_ID"
                + "          and ocp.version < ocpp.version)"
                + "  and NOT EXISTS (select pkcomp.version"
                + "        from PACKAGE_COM_PARAMETER pkcomp"
                + "        where pkcomp.PACK_ID = pkcp.PACK_ID"
                + "          and act.ACT_DATE >= pkcomp.PACK_COM_PDATE_APP"
                + "         and pkcomp.ENTITY_TYPE_ID = pkcp.ENTITY_TYPE_ID"
                + "        AND pkcp.VERSION < pkcomp.version)"
                + "  and NOT EXISTS (select prcomp.version"
                + "     from PRODUCT_COM_PARAMETER prcomp"
                + "   where prcomp.PRODUCT_ID = prcp.PRODUCT_ID"
                + "     and act.ACT_DATE >= prcomp.PROD_COM_PAPP_DATE"
                + "     and prcomp.ENTITY_TYPE_ID = prcp.ENTITY_TYPE_ID "
                + "      and prcp.VERSION < prcomp.version) ";

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection conn = DriverManager.getConnection(HOST + DB, USER, PASSWORD);

            PreparedStatement preparedSelect = conn.prepareStatement(sqlQuery);
            preparedSelect.setString(1, "14/07/2014");
            preparedSelect.setString(2, "16/08/2014");
            preparedSelect.setInt(3, 0);
            ResultSet rs = preparedSelect.executeQuery();

            while (rs.next()) {
                int ENTITY_TYPE_ID = rs.getInt(1);
                String REF_ICCID = rs.getString(2);
                String ACT_DATE = rs.getString(3);
                String STATE = rs.getString(4);
                String PDV_ENTITY_NAME = rs.getString(5);
                String CO_ID = rs.getString(6);
                String CONSP_AGREGATION = rs.getString(7);
                String CO_DEACTIV_DATE = rs.getString(8);
                int SIM_BOX_STATUS = rs.getInt(9);
                int CO_HOMOLOG_STATUS = rs.getInt(10);
                int PROD_COM_PID = rs.getInt(11);
                int PACK_COM_PID = rs.getInt(12);
                String PRODUCT_ID = rs.getString(13);
                String PACK_ID = rs.getString(14);
                long OFF_COM_PID = rs.getLong(15);
                String OFFER_ID = rs.getString(16);
                int PACK_COM_PPAIE = rs.getInt(17);
                int PACK_COM_PCOM = rs.getInt(18);
                int PROD_COM_PPAIE = rs.getInt(19);
                int COMPUTE_ID = rs.getInt(20);
                int COMPUTE_VERSION = rs.getInt(21);
                int OFF_COM_PCOMMISSION_PERIOD = rs.getInt(22);
                String ENTITY_CAT_ID = rs.getString(23);
                String DIST_ENTITY_NAME = rs.getString(24);
                int V1_COMPUTE_ID = rs.getInt(25);
                String COM_CONSO_VALUE = rs.getString(26);
                String COM_OFFER_MAX_THRESHOLD = rs.getString(27);
                String COM_OFFER_MAX_VALUE = rs.getString(28);
                String COM_OFFER_MIN_THRESHOLD = rs.getString(29);
                String COM_OFFER_MIN_VALUE = rs.getString(30);
                String PROD_COM_PCOM = rs.getString(31);
                String COMMISSION_SIM = rs.getString(32);
                String COMMISSION_SHARING = rs.getString(33);
                String COMMISSION_ADD = rs.getString(34);
                String COMMISSION_ADD_PDV = rs.getString(35);
                String COMMISSION_REPAYEMENT_SIM = rs.getString(36);
                String V1_COMMISSION_SIM = rs.getString(37);
                String V1_COMMISSION_SHARING = rs.getString(38);
                String V1_COMMISSION_ADD = rs.getString(39);
                String V1_COMMISSION_ADD_PDV = rs.getString(40);
                String V1_COMMISSION_REPAYEMENT_SIM = rs.getString(41);
                String CONSO_M = rs.getString(42);
                String CONSO_M1 = rs.getString(43);
                String CONSO_M2 = rs.getString(44);
                String CONSO_M3 = rs.getString(45);

                System.out.println(CONSO_M3 + "," + CONSO_M2 + "," + CONSO_M1 + "," + CONSO_M + "," + COMMISSION_ADD + "," + COMMISSION_ADD_PDV + "," + COMMISSION_REPAYEMENT_SIM + "," + V1_COMMISSION_SIM + "," + V1_COMMISSION_SHARING + "," + V1_COMMISSION_ADD + "," + V1_COMMISSION_ADD_PDV + "," + V1_COMMISSION_REPAYEMENT_SIM + "," + COMMISSION_SHARING + "," + COMMISSION_SIM + "," + PROD_COM_PCOM + "," + COM_OFFER_MIN_VALUE + "," + COM_OFFER_MIN_THRESHOLD + "," + COM_OFFER_MAX_VALUE + "," + COM_OFFER_MAX_THRESHOLD + "," + COM_CONSO_VALUE + "," + CO_DEACTIV_DATE + "," + ACT_DATE + "," + DIST_ENTITY_NAME + "," + V1_COMPUTE_ID + "," + ENTITY_CAT_ID + "," + STATE + "," + ENTITY_TYPE_ID + "," + OFFER_ID + "," + REF_ICCID + "," + CONSP_AGREGATION + "," + PDV_ENTITY_NAME + "," + CO_ID + "," + SIM_BOX_STATUS + "," + OFF_COM_PID + "," + CO_HOMOLOG_STATUS + "," + PROD_COM_PID + "," + PACK_COM_PID + "," + PRODUCT_ID + "," + PACK_ID + "," + PACK_COM_PPAIE + "," + PACK_COM_PCOM + "," + PROD_COM_PPAIE + "," + COMPUTE_ID + "," + COMPUTE_VERSION + "," + OFF_COM_PCOMMISSION_PERIOD);

            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
