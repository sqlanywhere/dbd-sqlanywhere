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

begin
    require 'rubygems'
    gem 'sqlanywhere'
    gem 'dbi'
rescue LoadError => e
end

require 'dbi'
require 'dbi/typeutil'
require 'sqlanywhere'
require 'singleton'

module DBI
module DBD
module SQLAnywhere

   VERSION = "1.0.0"

   def self.driver_name
      "SQLAnywhere"
   end

   class SA
      include Singleton
      attr_accessor :api

      def initialize
	 unless defined? SQLAnywhere
	   require 'sqlanywhere'
	 end

	 @api = ::SQLAnywhere::SQLAnywhereInterface.new()
	 result = ::SQLAnywhere::API.sqlany_initialize_interface( @api )
	 if result == 0 
	     raise LoadError, "Could not load SQLAnywhere DLL"
	 end
	 result = @api.sqlany_init()
	 if result == 0 
	     raise LoadError, "Could not initialize SQLAnywhere DLL"
	 end
      end

      def free_api
	 ::SQLAnywhere::API.sqlany_finalize_interface( @api );
	 @api = nil
      end
   end   


   DBI::TypeUtil.register_conversion(driver_name) do |obj|
       case obj
       when DBI::Binary # these need to be handled specially by the driver
	   obj.to_s
       when ::NilClass
	   nil
       when ::TrueClass
	   1
       when ::FalseClass
	   0
       when ::Time
	   obj.strftime("%H:%M:%S")
       when ::Date
	   obj.strftime("%Y/%m/%d")
       when ::DateTime, DBI::Timestamp
	   DateTime.parse(obj.to_s).strftime("%Y/%m/%d %H:%M:%S")	
       when ::String
	   obj
       when ::BigDecimal
	   obj.to_s("F")
       when ::Numeric
	   obj.to_s
       else
	   obj
       end
   end


   # This module provides functionality that is used by all the DBD classes
   module Utility

      NO_DIRECTION = 0
      INPUT_ONLY   = 1
      OUTPUT_ONLY  = 2
      INPUT_OUTPUT = 3

      # do_bind takes the following arguments:
      # * +prep_stmt+ : a handle the prepared Statement object
      # * +param+     : the parameter to bound, obtained by sqlany_describe_bind_param
      # * +bindvar+   : the actual value to bind the the parameter. Can be a +VALUE+, or a +HASH+.
      # * +i+         : the parameter number to bind. Should be the same as used in sqlany_describe_bind_param
      # * +bound+     : hash used to track INOUT, and OUT parameters
      #
      # +IN+ parameters will be bound once with +INPUT_ONLY+.
      # +OUT+ parameters will be bound once with +OUTPUT_ONLY+.
      # +INOUT+ parameters will be be bound twice, once as +INPUT_ONLY+, and once as +OUTPUT_ONLY+. +INOUT+ parameters 
      # will use *different* buffers to pass the input and output values to the DLL.
      #
      # If the parameter to be bound is +INPUT_ONLY+, +bindvar+ *must* be a regular value type such as
      # Bignum, Fixnum, String, etc. This value will be bound to the input parameter
      #
      # If the parameter to be bound is +OUTPUT_ONLY+, +bindvar+ *must* be a hash with keys:
      # ::name => This is the name that you will be used later to retrieve the output value
      # ::length => If the output will be a string or binary, the expected length must be stated. If this length is exceeded
      #    a DatabaseError (truncation) will be raised.
      #
      # If the parameter to be bound is +INPUT_OUTPUT+, +bindvar+ *must* be a hash with keys:
      # ::name => This is the name that you will be used later to retrieve the output value
      # ::value => The value to bind to the input.
      # ::length => If the output will be a string or binary, the expected length must be stated. If this length is exceeded
      #    a DatabaseError (truncation) will be raised.
      # 
      def do_bind!(prep_stmt, param, bindvar, i, bound)
	 # Get the direction
	 orig_direction = param.get_direction;

	 # Bind INPUT
	 if orig_direction == INPUT_ONLY or orig_direction == INPUT_OUTPUT
	    param.set_direction(INPUT_ONLY)
	    # Obtain the value out of the hash if neccessary
	    if bindvar.class == Hash
	       raise DBI::ProgrammingError.new("Parameter hash must contain :value key") if !bindvar.has_key?(:value)
	       param.set_value(bindvar[:value])
	    else
	       param.set_value(bindvar)
	    end
	    raise error() if SA.instance.api.sqlany_bind_param(prep_stmt, i, param) == 0
	 end

	 # Bind OUTPUT
	 if orig_direction == OUTPUT_ONLY or orig_direction == INPUT_OUTPUT
	    param.set_direction(OUTPUT_ONLY)
	    # Add the +::name+ to the +bound+ hash so its output value can be retrieved later
	    raise DBI::ProgrammingError.new("Parameter hash must contain :name key") if !bindvar.has_key?(:name)
	    bound[bindvar[:name]] = i if !bound.nil?
	    # set the buffer length if appropriate
	    if bindvar.has_key?(:length)
	       param.set_buffer_size(bindvar[:length])
	    end
	    # +set_value+ sets up the receiveing buffer
	    param.set_value(nil)
	    raise error() if SA.instance.api.sqlany_bind_param(prep_stmt, i, param) == 0
	 end
      end
   end

end # module SQLAnywhere
end # module DBD
end # module DBI

require 'dbd/sqlanywhere/driver'
require 'dbd/sqlanywhere/database'
require 'dbd/sqlanywhere/statement'
