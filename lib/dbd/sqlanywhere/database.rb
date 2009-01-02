#====================================================
#
#    Copyright 2008-2009 iAnywhere Solutions, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#                                                                               
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.
#
# While not a requirement of the license, if you do modify this file, we
# would appreciate hearing about it.   Please email sqlany_interfaces@sybase.com
#
#
#====================================================

module DBI::DBD::SQLAnywhere
   class Database < DBI::BaseDatabase
      include Utility

COLUMN_SELECT_STRING = <<END_OF_STATEMENT       
SELECT SYS.SYSTABCOL.column_name,
       SYS.SYSIDXCOL.sequence,
       SYS.SYSTABCOL."nulls",
       SYS.SYSTABCOL."default",
       SYS.SYSTABCOL.scale, 
       SYS.SYSTABCOL.width,
       SYS.SYSDOMAIN.type_id,
       SYS.SYSDOMAIN.domain_name,
       SYS.SYSIDX."unique"
FROM   (SYS.SYSTABLE join SYS.SYSTABCOL join SYS.SYSDOMAIN) left outer join (SYS.SYSIDXCOL join SYS.SYSIDX)
WHERE  table_name = ?
END_OF_STATEMENT

TABLE_SELECT_STRING = <<END_OF_STATEMENT       
SELECT table_name 
FROM SYS.SYSUSER, SYS.SYSTAB 
WHERE user_name not in ('SYS', 'dbo', 'rs_systabgroup') 
AND SYS.SYSUSER.user_id = SYS.SYSTAB.creator
END_OF_STATEMENT

      SQLANY_to_DBI = {
	   5 => DBI::SQL_SMALLINT,
	   4 => DBI::SQL_INTEGER,
	   2 => DBI::SQL_NUMERIC,
	   7 => DBI::SQL_FLOAT,
	   8 => DBI::SQL_DOUBLE,
	   9 => DBI::SQL_DATE,
	   1 => DBI::SQL_CHAR,
	  12 => DBI::SQL_VARCHAR,
	  -1 => DBI::SQL_LONGVARCHAR,
	  -2 => DBI::SQL_BINARY,
	  -4 => DBI::SQL_LONGVARBINARY,
	  11 => DBI::SQL_TIMESTAMP,
	  10 => DBI::SQL_TIME,
	  -6 => DBI::SQL_TINYINT,
	  -5 => DBI::SQL_BIGINT,
	  -9 => DBI::SQL_INTEGER, 
	 -10 => DBI::SQL_SMALLINT,
	 -11 => DBI::SQL_BIGINT,
	  -7 => DBI::SQL_BIT,
	   2 => DBI::SQL_DECIMAL,
	  -2 => DBI::SQL_VARBINARY,
	 -15 => DBI::SQL_BINARY,
	 -16 => DBI::SQL_VARBINARY,
	 -17 => DBI::SQL_LONGVARBINARY,
	 -18 => DBI::SQL_LONGVARCHAR,
	 -12 => DBI::SQL_CHAR,
	 -13 => DBI::SQL_VARCHAR,
	 -14 => DBI::SQL_LONGVARCHAR,
      }


      def disconnect
	 SA.instance.api.sqlany_rollback(@handle)
	 SA.instance.api.sqlany_disconnect(@handle)
	 SA.instance.api.sqlany_free_connection(@handle)
      end

      def prepare( statement )
	 stmt = SA.instance.api.sqlany_prepare(@handle, statement) 
	 if stmt.nil?
	    raise error()
	 else
	    return Statement.new(stmt, @handle)
	 end
      end

      def ping
	 res = SA.instance.api.sqlany_execute_immediate(@handle, 'SELECT * FROM dummy')
	 raise error() if res == 0
	 return res
      end
      
      def commit
	 res = SA.instance.api.sqlany_commit(@handle)
	 raise error() if res == 0
	 return res      
      end

      def rollback
	 res = SA.instance.api.sqlany_rollback(@handle)     
	 raise error() if res == 0
	 return res
      end

      def quote(value)
	 value.gsub("'", "''")
      end

      def execute(sql, *bindvars)
	 bound = {}
	 prep_stmt = SA.instance.api.sqlany_prepare(@handle, sql);
	 raise error() if prep_stmt.nil?
	 num_params = SA.instance.api.sqlany_num_params(prep_stmt)
	 raise error("Wrong number of parameters. Supplied #{bindvars.length} but expecting #{num_params}") if (num_params != bindvars.length)	 
	 num_params.times do |i|
	    res, param = SA.instance.api.sqlany_describe_bind_param(prep_stmt, i)
	    raise error() if res == 0 or param.nil?
	    do_bind!(prep_stmt, param, bindvars[i], i, bound)
	    param.finish
	 end

	 res = SA.instance.api.sqlany_execute(prep_stmt)
	 raise error() if res == 0   
	 return Statement.new(prep_stmt, @handle, bound)
      end

      def do(sql, *bindvars)
	 prep_stmt = SA.instance.api.sqlany_prepare(@handle, sql);
	 raise error() if prep_stmt.nil?
	 num_params = SA.instance.api.sqlany_num_params(prep_stmt)    
	 raise error("Wrong number of parameters. Supplied #{bindvars.length} but expecting #{num_params}") if (num_params != bindvars.length) 
	 num_params.times do |i|      
	    res, param = SA.instance.api.sqlany_describe_bind_param(prep_stmt, i)
	    raise error() if res == 0 or param.nil?
	    do_bind!(prep_stmt, param, bindvars[i], i, nil)
	    param.finish
	 end
	 res  = SA.instance.api.sqlany_execute(prep_stmt)
	 raise error() if res == 0
	 affected_rows = SA.instance.api.sqlany_affected_rows(prep_stmt)
	 return affected_rows
      end

      def tables
	 rs = SA.instance.api.sqlany_execute_direct(@handle, TABLE_SELECT_STRING)
	 if rs.nil?
	    return nil
	 else
	    tables = []
	    while (SA.instance.api.sqlany_fetch_next(rs) == 1)
	       res, cols = SA.instance.api.sqlany_get_column(rs, 0) 
	       raise error() if res == 0 or cols.nil?
	       tables << cols
	    end

	    return tables
	 end      
      end

      def columns( table )
	 prep_stmt = SA.instance.api.sqlany_prepare(@handle, COLUMN_SELECT_STRING)
	 raise error() if prep_stmt.nil?
	 res, param = SA.instance.api.sqlany_describe_bind_param(prep_stmt, 0)
	 raise error() if res == 0 or param.nil?
	 param.set_value(table) 
	 
	 raise error() if SA.instance.api.sqlany_bind_param(prep_stmt, 0, param) == 0
		     
	 res = SA.instance.api.sqlany_execute(prep_stmt)     

	 raise error() if res == 0 or prep_stmt.nil?
	 columns = []
	 col_count = 0
	 while (SA.instance.api.sqlany_fetch_next(prep_stmt) == 1)
	    columns << {}

	    res, col_val = SA.instance.api.sqlany_get_column(prep_stmt, 0)
	    raise error() if res == 0
	    columns[col_count]['name'] = col_val

	    res, col_val = SA.instance.api.sqlany_get_column(prep_stmt, 1)
	    raise error() if res == 0
	    columns[col_count]['pkey'] = !col_val.nil?

	    res, col_val = SA.instance.api.sqlany_get_column(prep_stmt, 2)
	    raise error() if res == 0
	    if col_val == 'Y'
	       columns[col_count]['nullable'] = true 
	    else 
	       columns[col_count]['nullable'] = false 
	    end	 

	    res, col_val = SA.instance.api.sqlany_get_column(prep_stmt, 3)
	    raise error() if res == 0
	    columns[col_count]['default'] = col_val

	    res, col_val = SA.instance.api.sqlany_get_column(prep_stmt, 4)
	    raise error() if res == 0
	    columns[col_count]['scale'] = col_val
	    
	    res, col_val = SA.instance.api.sqlany_get_column(prep_stmt, 5)
	    raise error() if res == 0
	    columns[col_count]['precision'] = col_val

	    res, col_val = SA.instance.api.sqlany_get_column(prep_stmt, 6)
	    raise error() if res == 0 
	    columns[col_count]['sql_type'] = SQLANY_to_DBI[col_val]

	    res, col_val = SA.instance.api.sqlany_get_column(prep_stmt, 7)
	    raise error() if res == 0
	    columns[col_count]['type_name'] = col_val.downcase

	    res, col_val = SA.instance.api.sqlany_get_column(prep_stmt, 8)
	    raise error() if res == 0
	    columns[col_count]['unique'] = (col_val == 1 or col_val == 2)

	    col_count += 1	 
	 end
	 param.finish
	 return columns
      end

      def [] (attr)
	 @attr[attr]
      end
      
      def []= (attr, val)
	 @attr[attr] = val
      end

      protected
	 def error(*custom_msg)
	    code, msg = SA.instance.api.sqlany_error(@handle)
	    state = SA.instance.api.sqlany_sqlstate(@handle)
	    SA.instance.api.sqlany_clear_error(@handle)
	    if !custom_msg.nil?
		if custom_msg.length != 0
		    msg = "#{custom_msg}. #{msg}"
		end
	    end
	    return DBI::DatabaseError.new(msg, code, state)
	 end
   end

end
