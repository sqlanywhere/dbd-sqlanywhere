#====================================================
#
#    Copyright 2012 iAnywhere Solutions, Inc.
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
   class Statement < DBI::BaseStatement
      include Utility


      # Conversion table between SQL Anywhere E-SQL types and DBI SQL Types
      SQLANY_NATIVE_TYPES = {
	 0   => DBI::SQL_LONGVARCHAR,
	 384 => DBI::SQL_DATE,
	 388 => DBI::SQL_TIME,
	 392 => DBI::SQL_TIMESTAMP,
	 448 => DBI::SQL_VARCHAR,
	 452 => DBI::SQL_CHAR,
	 456 => DBI::SQL_LONGVARCHAR,
	 460 => DBI::SQL_LONGVARCHAR,
	 480 => DBI::SQL_DOUBLE,
	 482 => DBI::SQL_FLOAT,
	 484 => DBI::SQL_DECIMAL,
	 496 => DBI::SQL_INTEGER,
	 500 => DBI::SQL_SMALLINT,
	 524 => DBI::SQL_BINARY,
	 528 => DBI::SQL_LONGVARBINARY,
	 604 => DBI::SQL_TINYINT,
	 608 => DBI::SQL_BIGINT,
	 612 => DBI::SQL_INTEGER,
	 616 => DBI::SQL_SMALLINT,
	 620 => DBI::SQL_BIGINT,
	 624 => DBI::SQL_BIT,
	 640 => DBI::SQL_LONGVARCHAR
      }

      def initialize(handle, conn, bound = {} )
	 @handle = handle
	 @conn = conn
	 @arr = []
	 @bound = bound
	 @offset = -1
      end

      def __bound_param(name)
	 if !@bound[name].nil?
	    res, param = SA.instance.api.sqlany_get_bind_param_info(@handle, @bound[name])
	    raise error() if res == 0
	    return param.get_output()
	 end 
      end

      # Although SQL Anywhere allows multiple result sets,
      # the @fetchable variable disallows there use
      #
      #def __next_resultset
      #   return SA.instance.api.sqlany_get_next_result(@handle)
      #end

      def bind_param(param, value, attribs)
	 param -= 1
	 res, param_description = SA.instance.api.sqlany_describe_bind_param(@handle, param)
	 raise error() if res == 0 or param_description.nil?
	 do_bind!(@handle, param_description, value, param, @bound)
	 param_description.finish
      end

      def execute()
	 res = SA.instance.api.sqlany_execute(@handle)
	 raise error() if res == 0
      end

      def fetch()
	 return fetch_scroll(DBI::SQL_FETCH_NEXT, 1)  
      end

      def fetch_all()
	 rows = []
	 loop {
	    new_row = self.fetch_scroll(DBI::SQL_FETCH_NEXT, 1)
	    break if new_row.nil?
	    rows << new_row.clone
	 }
	 return rows
      end

      def fetch_scroll(direction, offset)
	 res = 0
	 new_offset = @offset

	 case direction
	    when DBI::SQL_FETCH_NEXT
	       res = SA.instance.api.sqlany_fetch_next(@handle)
	       new_offset += 1
	    when DBI::SQL_FETCH_PRIOR
	       res = SA.instance.api.sqlany_fetch_absolute(@handle, @offset)
	       new_offset -= 1
	    when DBI::SQL_FETCH_FIRST
	       res = SA.instance.api.sqlany_fetch_absolute(@handle, 1)
	       new_offset = 0
	    when DBI::SQL_FETCH_LAST
	       res = SA.instance.api.sqlany_fetch_absolute(@handle, -1)
	       new_offset = self.rows() - 1
	    when DBI::SQL_FETCH_ABSOLUTE
	       res = SA.instance.api.sqlany_fetch_absolute(@handle, offset)
	       if offset <= 0
		  new_offset = self.rows() + offset 
	       else
		  new_offset = offset - 1
	       end
	    when DBI::SQL_FETCH_RELATIVE
	       res = SA.instance.api.sqlany_fetch_absolute(@handle, @offset + offset + 1)
	       new_offset += offset
	 end

	 if (res == 1)
	    retrieve_row_data()
	    @offset = new_offset
	    return @arr
	 else
	    return nil
	 end   
      end

      def column_info
	 columns = []
	 if !@handle.nil?
	    max_cols = SA.instance.api.sqlany_num_cols(@handle)
	    raise error() if max_cols == -1
	    max_cols.times do |cols|
	       columns << {}
	       res, holder, col_name, type, native_type, precision, scale, max_size, nullable = SA.instance.api.sqlany_get_column_info(@handle, cols)
	       raise error() if res == 0 or col_name.nil?
	       columns[cols]["name"] = col_name
	       sql_type = SQLANY_NATIVE_TYPES[native_type]
	       columns[cols]["sql_type"] = sql_type
	       columns[cols]["type_name"] = DBI::SQL_TYPE_NAMES[sql_type]
               if [ DBI::SQL_CHAR, DBI::SQL_VARCHAR,
                    DBI::SQL_BINARY, DBI::SQL_VARBINARY ].include?(sql_type)
                   precision = max_size
               end
               if precision != 0 or scale != 0
                   columns[cols]["precision"] = precision
                   columns[cols]["scale"] = scale
               end
	       columns[cols]["nullable"] = (nullable == 0)

	       columns[cols]["dbi_type"] = DBI::Type::Boolean if sql_type == DBI::SQL_BIT
	    end
	 end
	 return columns  
      end

      def rows
	 if !@handle.nil?
	    res = SA.instance.api.sqlany_affected_rows(@handle)
	    raise error() if res == -1
	    return res	 
	 else
	    0
	 end
      end

      def finish
	 if !@handle.nil?
	    SA.instance.api.sqlany_free_stmt(@handle);
	    @handle = nil
	 end
      end
      
      def cancel
      end

      protected
	 def error(*custom_msg)
	    code, msg = SA.instance.api.sqlany_error(@conn)
	    state = SA.instance.api.sqlany_sqlstate(@conn)
	    SA.instance.api.sqlany_clear_error(@conn)
	    if !custom_msg.nil?
		if custom_msg.length != 0
		    msg = "#{custom_msg}. #{msg}"
		end
	    end
	    return DBI::DatabaseError.new(msg, code, state)
	 end

	 def retrieve_row_data
	    max_cols = SA.instance.api.sqlany_num_cols(@handle)
	    raise error() if max_cols == -1
	    max_cols.times do |cols|
	       res, col_val = SA.instance.api.sqlany_get_column(@handle, cols)
	       raise error() if res == 0
	       @arr[cols] = ( col_val ? col_val.to_s : nil )
	    end
	 end
   end
end
