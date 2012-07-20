<?php

/**
 * Description of SqlDb
 *
 * @author Andrew Tereshko <andrew.tereshko@gmail.com>
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
        $this->driver = new \PDO(
                    $config['type'] . ':host=' . $config['host'] . ';dbname=' . $config['name'],
                    $config['user'],
                    $config['password']
            );

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
}
