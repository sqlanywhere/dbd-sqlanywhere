DBDConfig.set_testbase(:sqlanywhere, Class.new(Test::Unit::TestCase) do
        def dbtype 
            "sqlanywhere"
        end

        def test_base
            assert_equal(@dbh.driver_name, "SQLAnywhere")
            assert_kind_of(DBI::DBD::SQLAnywhere::Database, @dbh.instance_variable_get(:@handle))
        end

        def set_base_dbh
            config = DBDConfig.get_config["sqlanywhere"]
            @dbh = DBI.connect("dbi:SQLAnywhere:"+config["dbname"], config["username"], config["password"], { })
        end

        def setup
            set_base_dbh
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/sqlanywhere/up.sql")
        end

        def teardown
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/sqlanywhere/down.sql")
            @dbh.disconnect
        end
    end
)
