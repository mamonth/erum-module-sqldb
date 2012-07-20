<?php
namespace SqlDb;

/**
 * Value Object generator
 *
 * @uses \Sql\Reflection\Table
 * 
 * @author Andrew Tereshko <andrew.tereshko@gmail.com>
 */
class VOBuilder
{
    /**
     *
     * @var \Sql\Reflection\Table
     */
    protected $tableReflection;
    
    protected $namespace;
    
    public function __construct( $sqlFile )
    {
        if( !class_exists( '\Sql\Reflection\Table' ) )
        {
            throw new \SqlDb\Exception( 'Value Object generator requires Sql\Reflection library.' );
        }
        
        $sql = file_get_contents( $sqlFile );
    
        $this->tableReflection = \Sql\Reflection\Table::sqlFactory( $sql );
        
        $this->prepareData();
    }
    
    protected function prepareData()
    {
    }
    
    public function setNamespace( $namespace )
    {
        $this->namespace = $namespace;
    }
    
    public function getClassName()
    {
        $className = strtolower( $this->tableReflection->name );
        
        $className = str_ireplace( array( '_', '-' ), ' ', $className );
        
        $className = implode( '', array_map( 'ucfirst', explode( ' ', trim( $className ) ) ) );
        
        return $className;
    }
    
    public function save( $destDir )
    {
        ob_start();
        
        include dirname( __DIR__ ) . '/templates/ValueObject.tpl';
        
        $voClass = ob_get_clean();
        
        $destFIle = $destDir . '/' . $this->getClassName() . '.php';
        
        file_put_contents( $destFIle , $voClass );
    }
}
