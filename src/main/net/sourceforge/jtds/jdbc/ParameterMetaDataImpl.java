package net.sourceforge.jtds.jdbc;

import java.sql.*;

public class ParameterMetaDataImpl implements ParameterMetaData {
    private ParameterListItem[] parameterList;

    public ParameterMetaDataImpl(ParameterListItem[] parameterList) {
        if (parameterList == null) {
            parameterList = new ParameterListItem[0];
        }

        this.parameterList = parameterList;
    }

    public int getParameterCount() throws SQLException {
        return parameterList.length;
    }

    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    public int getParameterType(int param) throws SQLException {
        NotImplemented();
        return -1;
    }

    public int getScale(int param) throws SQLException {
        NotImplemented();
        return -1;
    }

    public boolean isSigned(int param) throws SQLException {
        NotImplemented();
        return false;
    }

    public int getPrecision(int param) throws SQLException {
        NotImplemented();
        return -1;
    }

    public String getParameterTypeName(int param) throws SQLException {
        NotImplemented();
        return null;
    }

    public String getParameterClassName(int param) throws SQLException {
        NotImplemented();
        return null;
    }

    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeUnknown;
    }

    private void NotImplemented() throws SQLException {
        throw new SQLException("Not Implemented.");
    }
}
