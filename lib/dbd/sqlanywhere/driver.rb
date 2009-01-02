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
   class Driver < DBI::BaseDriver
      include Utility

      def initialize
	 super("0.4.0")
      end

      def connect( dbname, user, auth, attr )
	 conn = SA.instance.api.sqlany_new_connection()
	 conn_str = "uid=#{user};pwd=#{auth};";

	 if !dbname.nil?
	    conn_str += "eng=#{dbname};";
	 end

	 attr.keys.each do |option|
	    conn_str += "#{option}=#{attr[option]};"
	 end

	 SA.instance.api.sqlany_connect(conn, conn_str)
	 return Database.new(conn, attr)      
      end

      def default_user
	 return ['dba', 'sql']
      end

      def disconnect_all
	 SA.instance.api.sqlany_fini()
	 SA.instance.free_api
      end
   end   
end
