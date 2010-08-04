#====================================================
#
#    Copyright 2008-2010 iAnywhere Solutions, Inc.
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

      # The DBNAME string can be specified in the following forms:
      #
      # "DBI:SQLAnywhere:"
      # "DBI:SQLAnywhere:{ENG}"
      # "DBI:SQLAnywhere:{ENG}:{DBN}"
      # "DBI:SQLAnywhere:{CONNECTION_STRING}"  # where CONNECTION_STRING ~= "key1=val1;key2=val2;..."
      #
      # For the first form, nothing will be added to the connection string. With the second and third forms
      # the driver will add ENG and DBN to the connection string accordingly. The fourth form will pass
      # the supplied connection string through unmolested.
      #
      # The user and auth can be passed into the function and they will be automatically appended to the
      # connection string. Since Ruby DBI will automatically fill in the username and password with defaults if they are ommited, 
      # you should NEVER include a "UID=" or "PWD=" in your connection string or an exception will be thrown.
      #
      # Examples:  
      #
      #   Function Call                                              ==> Generated Connection String
      #   ==============================================================================================
      #   DBI.connect("DBI:SQLAnywhere:")                            ==>  "uid=dba;pwd=sql"
      #   DBI.connect("DBI:SQLAnywhere:Demo")                        ==>  "eng=Demo;uid=dba;pwd=sql"
      #   DBI.connect("DBI:SQLAnywhere:Demo:Test")                   ==>  "eng=Demo;dbn=Test;uid=dba;pwd=sql"
      #   DBI.connect("DBI:SQLAnywhere:Demo:Test", 'john', 'doe')    ==>  "eng=Demo;dbn=Test;uid=john;pwd=doe"
      #   DBI.connect("DBI:SQLAnywhere:eng=Demo;dbn=Test")           ==>  "eng=Demo;dbn=Test;uid=dba;pwd=sql"
      #   DBI.connect("DBI:SQLAnywhere:eng=Demo;dbn=Test;uid=john")  ==>  EXCEPTION! UID cannot be specified in the connection string
      #   DBI.connect("DBI:SQLAnywhere:CommLinks=tcpip(port=2638)")  ==>  "CommLinks=tcpip(port=2638);uid=dba;pwd=sql"
      #
      # The attr parameter is ignored.

      def connect( dbname, user, auth, attr )
	 conn = SA.instance.api.sqlany_new_connection()

         conn_str = ''
         
         unless dbname.nil?
            if dbname =~ /^[^=:;]+$/
               conn_str = "eng=#{dbname};"
            elsif dbname =~ /^[^=:;]+:[^=:;]+$/
               eng_name, db_name = dbname.split(":")
               conn_str = "eng=#{eng_name};dbn=#{db_name};"
            else
               conn_str = dbname;
               conn_str << ';' unless conn_str.length == 0 or conn_str[conn_str.length - 1, 1] == ';'
            end
         end

         unless user.nil?
            raise DBI::ProgrammingError.new("UID is specified twice. Once in the connection string AND once as a connect() parameter. It can only be specified once.") if conn_str =~ /uid=/i
            conn_str << "uid=#{user};"
         end

         unless auth.nil?
            raise DBI::ProgrammingError.new("PWD is specified twice. Once in the connection string AND once as a connect() parameter. It can only be specifed once.") if conn_str =~ /pwd=/i
            conn_str << "pwd=#{auth};"
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
