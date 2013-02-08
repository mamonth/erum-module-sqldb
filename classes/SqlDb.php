<?php

/**
 * SQL database module for Erum framework
 *
 * @author Andrew Tereshko <andrew.tereshko@gmail.com>
 *
 * @method static SqlDb factory( $configAlias = 'default' )
 *
 * @method \PDOStatement query()
 */
class SqlDb extends \Erum\ModuleAbstract
{
    /**
     *
     * @var \PDO
     */
    private $driver;

    public function __construct( array $config )
    {
        $dsn = $config['type'] . ':host=' . $config['host'] . ';dbname=' . $config['name'];

        if( isset( $config['port'] ) ) $dsn .= ';port=' . $config['port'];

        $this->driver = new \PDO( $dsn, $config['user'], $config['password'] );

        $this->driver->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
    }

    public function __call( $method, $options )
    {
        return call_user_func_array( array( $this->driver, $method ), $options );
    }

    public static function sqlTimeFromUnix( $timestamp )
    {
        return date( 'Y-m-d H:i:s', (int)$timestamp );
    }

    public static function unixTimeFromSql( $sqlTime )
    {
        return \strtotime( $sqlTime );
    }

    /**
     * here extends PDO methods - to avoid __call
     * @todo - realize moduleInterface to avoid crunches
     */

    public function prepare( $statement ,array $driver_options = array() )
    {
        return $this->driver->prepare( $statement, $driver_options );
    }
}

