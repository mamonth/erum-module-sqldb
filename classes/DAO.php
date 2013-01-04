<?php
namespace SqlDb;

/**
 * Description of DAOAbstract
 * 
 * @author Andrew Tereshko <andrew.tereshko@gmail.com>
 */
abstract class DAO extends \Erum\DAOAbstract
{
    /**
     * Gets single model data by Id.
     *
     * @param mixed $modelId
     * @return ModelAbstract
     */
    public static function get( $modelId )
    {
        if ( !$modelId ) return null;

        $className = self::getModelClass( true );

        $properties = (array) $className::identityProperty();

        $modelId = (array) $modelId;

        if ( sizeof( $properties ) != sizeof( $modelId ) )
            throw new Exception( 'Model identity properties count do not equal count of values given.' );

        $model = \Erum\ModelWatcher::instance()->get( $className, $modelId );

        if ( null === $model )
        {
            $condition = new \Sql\Condition();

            foreach ( $properties as $property )
            {
                $condition
                        ->where( $property . ' = :' . $property )
                        ->bindValue( ':' . $property, array_shift( $modelId ) );
            }

            $table = self::getModelTable( $className );

            $query = 'SELECT ' . $table . '.* FROM ' . $table . ' ' . $condition;

            $stmt = \SqlDb::factory()->prepare( $query );
            $stmt->execute();

            $model = $stmt->fetchObject( $className );

            // normalize property values
            foreach( $model->tableReflection()->columns as $column )
            {
                $model->{$column->name} = self::castFromSql( $model->{$column->name}, $column->datatype );
            }
        }
        
        if( $model instanceof $className )
        {
            \Erum\ModelWatcher::instance()->bind( $model );
        }
        else
        {
            $model = null;
        }

        return $model;
    }
    
    /**
     * Gets models list by ?
     *
     * @todo implement Container objects support, implement Condition support
     *
     * @param string $condition
     * @return array
     */
    public static function getList( $condition = null )
    {
//        if ( $condition instanceof \Sql\Condition )
//        {
//            //$condition->from( $table );
//        }

        $className = self::getModelClass( true );

        $query = 'SELECT ' . self::getModelTable( $className ) . '.* FROM ' . self::getModelTable( $className ) . ( $condition ? ' ' . $condition : '');

        $stmt = \SqlDb::factory()->prepare( $query );
        
        $stmt->execute();

        $list = array( );

        while ( $model = $stmt->fetchObject( $className ) )
        {
            $list[ implode( ':', (array)$model->id ) ] = $model;

            // normalize property values
            foreach( $model->tableReflection()->columns as $column )
            {
                $model->{$column->name} = self::castFromSql( $model->{$column->name}, $column->datatype );
            }

            $oldModel = \Erum\ModelWatcher::instance()->get( $className, $model->id );
            
            if( $oldModel )
            {
                unset( $oldModel );
            }
            else
            {
                \Erum\ModelWatcher::instance()->bind( $model );
            }
            
        }

        return $list;
    }
    
    /**
     * Gets models list by ids
     *
     * @param mixed $condition
     * @return array
     */
    public static function getListByIds( array $ids )
    {
        if( !sizeof($ids) ) return array();

        $className = self::getModelClass( true );

        $properties = (array) $className::identityProperty();
        
        $propertiesCount = sizeof( $properties );
        
        $list = array();

        reset( $ids );
        while( list( $key, $modelId ) = each( $ids ) )
        {
            $modelId = (array)$modelId;
            
            if( sizeof( $modelId ) != $propertiesCount )
                throw new Exception( 'Model identity properties count do not equal count of values given.' );
            
            $model = \Erum\ModelWatcher::instance()->get( $className, $modelId );
            
            if( $model )
            {
                $list[ implode( ':', (array)$model->id ) ] = $model;
            }
        }
        
        $condition = new \Sql\Condition();
        
        for( $i = 0; $i < $propertiesCount; $i++ )
        {
            $idParts = array();
            
            reset( $ids );
            while( list( , $modelId ) =each( $ids ) )
            {
                if( isset( $list[ implode( ':', (array)$model->id ) ] ) ) continue;
                
                $idParts[] = (array)$modelId[ $i ];
            }
            
            $condition->where( $properties[ $i ] . ' IN ( ' . implode( ',', $idParts ) . ' ) ' );
        }
        unset( $idParts );
        
        $stmt = \SqlDb::factory()->prepare( 'SELECT * FROM ' . self::getModelTable() . $condition );
        
        $stmt->execute();

        while ( $model = $stmt->fetchObject( self::getModelClass( true ) ) )
        {
            $list[ implode( ':', (array)$model->id ) ] = $model;
            
            \Erum\ModelWatcher::instance()->bind( $model );

            // normalize property values
            foreach( $model->tableReflection()->columns as $column )
            {
                $model->{$column->name} = self::castFromSql( $model->{$column->name}, $column->datatype );
            }
        }
        
        return $list;
    }
    
    /**
     * Get model count in storage
     * 
     * @param bool | string | \Sql\Condition $condition
     */
    public static function getCount( $condition = false )
    {        
        $stmt = \SqlDb::factory()->query( 'SELECT COUNT(*) FROM ' . self::getModelTable() . ' ' . ( $condition ? $condition : '' ) );
        
        return $stmt->fetchColumn();
    }
    
    /**
     * Delete model data from storage by model Id.
     *
     * @param ModelAbstract $model
     * @return boolean
     */
    public static function delete( \Erum\ModelAbstract $model )
    {
        \Erum\ModelWatcher::instance()->unbind( get_class( $model ), $model->getId() );
        
        $stmt = \SqlDb::factory()->prepare( 'DELETE FROM ' . self::getModelTable() . ' WHERE id = :id LIMIT 1' );
        
        $stmt->execute( array( ':id' => (int) $model->id ) );

        return $stmt->rowCount() ? true : false;
    }

    public static function save( \Erum\ModelAbstract $model )
    {
        if( self::isNew( $model ) )
        {
            self::insert( $model );
        }
        else
        {
            self::update( $model );
        }
    }

    public static function insert( \Erum\ModelAbstract $model )
    {
        $columnNames    = array();
        $columnValues   = array();
        $modelClass     = \get_class( $model );

        foreach( $model->tableReflection()->columns as $column )
        {
            /* @var $column \Sql\Reflection\Column */

            // skip serial
            if( $column->datatype == 'SERIAL' ) continue;

            $columnNames[] = $column->name;

            $columnValues[ ':' . $column->name ] = self::castToSql( $model->{$column->name}, $column->datatype );
        };

        $sql = 'INSERT INTO ' . self::getModelTable( $modelClass )
            . '(' . \implode( ',', $columnNames )
            . ') VALUES (' . \implode( ',', array_keys( $columnValues ) )
            . ') RETURNING ' . \implode( ',', (array)$modelClass::identityProperty() ) . ';';

        echo $sql;

        $stmt = \SqlDb::factory()->prepare( $sql );

        $stmt->execute( $columnValues );

        $result = $stmt->fetch( \PDO::FETCH_ASSOC );

        foreach( $result as $column => $value )
        {
            $model->$column = $value;
        }

        return true;
    }

    public static function update( \Erum\ModelAbstract $model )
    {
        $columnSets     = array();
        $columnValues   = array();
        $modelClass     = \get_class( $model );
        $keyConditions  = array();

        foreach( (array)$modelClass::identityProperty() as $column )
        {
            $keyConditions[ $column ] = $column . ' = :' . $column;
        }

        foreach( $model->tableReflection()->columns as $column )
        {
            /* @var $column \Sql\Reflection\Column */

            if( !isset( $keyConditions[ $column->name ] ) )
            {
                $columnSets[] = $column->name . ' = ' . ':' . $column->name;
            }

            $columnValues[ ':' . $column->name ] = self::castToSql( $model->{$column->name}, $column->datatype );
        }

        $sql = 'UPDATE ' . self::getModelTable( $modelClass )
            . ' SET ' . \implode( ',', $columnSets )
            . ' WHERE ' . \implode( ' AND ', $keyConditions );

        $stmt = \SqlDb::factory()->prepare( $sql );

        $stmt->execute( $columnValues );
    }

    /**
     * Find out - is model new (need insert) or existed (need update)
     *
     * @param \Erum\ModelAbstract $model
     * @return bool
     */
    public static function isNew( \Erum\ModelAbstract $model )
    {
        if( $model->getId() )
        {
            $properties = (array)$model->identityProperty();

            if( sizeof( $properties ) > 1 )
            {
                foreach( $properties as $property )
                {
                    if( null === $model->__get( $property ) )
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        return true;
    }

    /**
     * Get's model table
     * 
     * @param string $className
     * @return string 
     */
    public static function getModelTable( $className = null )
    {
        if ( null === $className )
            $className = self::getModelClass( true );

        $reflection = new \ReflectionClass( $className );

        if ( !$reflection->isSubclassOf( '\Erum\ModelAbstract' ) )
            throw new \Exception( 'Only ModelAbstract child can be returned !' );
        
        if ( $reflection->getConstant( 'table' ) )
        {
            $table = $className::table;
        }
        else
        {
            $table = substr( strrchr( strtolower( $className ) , '\\' ), 1 );
        }

        return $table;
    }

    public static function castToSql( $value, $columnType = 'VARCHAR' )
    {
        if( null === $value ) return null;

        // prepare arrays
        if( ']' == substr( $columnType, -1, 1 ) )
        {
            $columnType = 'ARRAY';
        }

        switch( $columnType )
        {
            case 'INT':
            case 'SMALLINT':
            case 'BIGINT':
            case 'INTEGER':
                $value = (int)$value;
                break;
            case 'BOOLEAN':
            case 'BOOL':
                $value = (bool)$value;
                break;
            case 'ARRAY':
                $value = self::arr2pgarr( $value );
                break;
            default:
        }

        return $value;
    }

    public static function castFromSql( $value, $columnType = 'VARCHAR' )
    {
        if( null === $value ) return null;

        // prepare arrays
        if( ']' == substr( $columnType, -1, 1 ) )
        {
            $columnType = 'ARRAY';
        }

        switch( $columnType )
        {
            case 'INT':
            case 'SMALLINT':
            case 'BIGINT':
            case 'INTEGER':
            case 'SERIAL':
                $value = (int)$value;
                break;
            case 'BOOLEAN':
            case 'BOOL':
                $value = (bool)$value;
                break;
            case 'ARRAY':

                $value = self::pgarr2arr( $value );
                break;
            default:
        }

        return $value;
    }

    // Here comes tool parts
    // @TODO need to be relocated to database-depended location

    public static function arr2pgarr( $value )
    {
        $parts = array();

        foreach ( (array)$value as $inner)
        {
            if ( is_array($inner) )
            {
                $parts[] = self::arr2pgarr( $inner );
            }
            elseif ($inner === null)
            {
                $parts[] = 'NULL';
            }
            else
            {
                $parts[] = '"' . addcslashes($inner, "\"\\") . '"';
            }
        }

        return '{' . join(",", (array)$parts) . '}';
    }


    public static function pgarr2arr($str, $start=0)
    {
        static $p;
        if ($start==0) $p=0;
        $result = array();

        // Leading "{".
        $p += strspn($str, " \t\r\n", $p);
        $c = substr($str, $p, 1);

        if ($c != '{') {
            return;
        }
        $p++;

        // Array may contain:
        // - "-quoted strings
        // - unquoted strings (before first "," or "}")
        // - sub-arrays
        while (1) {
            $p += strspn($str, " \t\r\n", $p);
            $c = substr($str, $p, 1);

            // End of array.
            if ($c == '}') {
                $p++;
                break;
            }

            // Next element.
            if ($c == ',') {
                $p++;
                continue;
            }

            // Sub-array.
            if ($c == '{')
            {
                $result[] = self::pgarr2arr($str, $p);
                continue;
            }

            // Unquoted string.
            if ($c != '"')
            {
                $len    = strcspn($str, ",}", $p);
                $v      = stripcslashes(substr($str, $p, $len));

                if (!strcasecmp($v, "null"))
                {
                    $result[] = null;
                }
                else
                {
                    $result[] = $v;
                }
                $p += $len;
                continue;
            }

            // Quoted string.
            $m = null;
            if (preg_match('/" ((?' . '>[^"\\\\]+|\\\\.)*) "/Asx', $str, $m, 0, $p))
            {
                $result[] = stripcslashes($m[1]);
                $p += strlen($m[0]);
                continue;
            }
        }

        return $result;

    }
    
}
