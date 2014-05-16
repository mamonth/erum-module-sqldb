<?php
namespace SqlDb;

/**
 * Description of DAOAbstract
 * 
 * @author Andrew Tereshko <andrew.tereshko@gmail.com>
 */

use \Erum\ModelWatcher,
    \Erum\ModelAbstract;

abstract class DAO extends \Erum\DAOAbstract
{
    /**
     * Gets single model data by Id.
     *
     * @param mixed $modelId
     * @throws Exception
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

        $model = ModelWatcher::instance()->get( $className, $modelId );

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

            if( $model instanceof $className )
            {
                // normalize property values
                foreach( $model->tableReflection()->columns as $column )
                {
                    $model->{$column->name} = self::castFromSql( $model->{$column->name}, $column->datatype );
                }

                ModelWatcher::instance()->bind( $model );
            }
            else
            {
                $model = null;
            }
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

        $list           = array( );
        $tableColumns   = array();

        while ( $modelData = $stmt->fetch( \PDO::FETCH_ASSOC ) )
        {
            $model = new $className;

            if( !isset( $tableColumns ) )
            {
                $tableColumns = $model->tableReflection()->columns;
            }

            // normalize && set property values
            foreach( $model->tableReflection()->columns as $column )
            {
                $model->{$column->name} = self::castFromSql( $modelData[ $column->name ], $column->datatype );
            }

            $oldModel = ModelWatcher::instance()->get( $className, $model->id );
            
            if( $oldModel )
            {
                unset( $oldModel );
                ModelWatcher::instance()->unbind( $className, $model->id );
            }

            ModelWatcher::instance()->bind( $model );

            $list[ implode( ':', (array)$model->id ) ] = $model;

        }

        return $list;
    }

    /**
     * Gets models list by ids
     *
     * @param array $ids
     *
     * @throws Exception
     * @internal param mixed $condition
     * @return array
     */
    public static function getListByIds( array $ids )
    {
        if( !sizeof($ids) ) return array();

        $className = self::getModelClass( true );

        $properties = (array) $className::identityProperty();
        
        $propertiesCount = sizeof( $properties );
        
        $list       = array();
        $sortIds    = array();

        foreach( $ids as &$modelId )
        {
            $modelId = is_array( $modelId ) ? $modelId : array( $modelId );

            if( sizeof( $modelId ) != $propertiesCount )
                throw new Exception( 'Model identity properties count do not equal count of values given.' );

            $model = ModelWatcher::instance()->get( $className, $modelId );

            if( $model )
            {
                $list[ $model->getId() ] = $model;
            }

            $sortIds[] = is_array( $modelId ) ? implode(':', $modelId) : $modelId;
        }

        $condition = new \Sql\Condition();

        if( sizeof( $ids ) !== sizeof( $list ) )
        {
            for( $i = 0; $i < $propertiesCount; $i++ )
            {
                $idParts = array();

                foreach( $ids as &$modelId )
                {
                    if( isset( $list[ implode( ':', is_array( $modelId ) ? $modelId : array( $modelId ) ) ] ) ) continue;

                    $idParts[] = $modelId[ $i ];
                }

                if( !empty( $idParts ) )
                {
                    $condition->where( $properties[ $i ] . ' IN ( ' . implode( ',', $idParts ) . ' ) ' );
                }
            }
            unset( $idParts );

            $stmt = \SqlDb::factory()->prepare( 'SELECT * FROM ' . self::getModelTable() . ' ' . $condition );

            $stmt->execute();

            $className      = self::getModelClass();
            $tableColumns   = null;

            while ( $modelData = $stmt->fetch( \PDO::FETCH_ASSOC ) )
            {
                $model = new $className;

                if( null === $tableColumns )
                {
                    $tableColumns = $model->tableReflection()->columns;
                }

                // set && normalize property values
                foreach( $tableColumns as $column )
                {
                    $model->{$column->name} = self::castFromSql( $modelData[ $column->name ], $column->datatype );
                }

                $list[ implode( ':', is_array( $model->id ) ? $model->id : array( $model->id ) ) ] = $model;

                ModelWatcher::instance()->bind( $model );
            }

            // sort with initial ids order
            $list = \Erum\Arr::ksortByArray( $list, $sortIds );
        }

        return $list;
    }
    
    /**
     * Get model count in storage
     * 
     * @param bool | string | \Sql\Condition $condition
     * @return integer
     */
    public static function getCount( $condition = false )
    {
        if ( $condition instanceof \Sql\Condition )
        {
            $condition = clone $condition;

            $condition->dropOrder();
        }

        $stmt = \SqlDb::factory()->query( 'SELECT COUNT(*) FROM ' . self::getModelTable() . ' ' . ( $condition ? $condition : '' ) );
        
        return (int)$stmt->fetchColumn();
    }

    /**
     * Delete model data from storage by model Id.
     *
     * @param ModelAbstract $model
     *
     * @return boolean
     */
    public static function delete( ModelAbstract $model )
    {
        ModelWatcher::instance()->unbind( get_class( $model ), $model->getId() );
        
        $stmt = \SqlDb::factory()->prepare( 'DELETE FROM ' . self::getModelTable() . ' WHERE id = :id' );
        
        $stmt->execute( array( ':id' => (int) $model->id ) );

        return $stmt->rowCount() ? true : false;
    }

    /**
     * @param ModelAbstract $model
     *
     * @return boolean
     */
    public static function save( ModelAbstract $model )
    {
        if( self::isNew( $model ) )
        {
            return static::insert( $model );
        }
        else
        {
            return static::update( $model );
        }
    }

    /**
     * Save model collection
     *
     * @param ModelAbstract[] $modelList
     */
    public static function saveList( array $modelList )
    {
        $listInsert = array();
        $listUpdate = array();

        foreach( $modelList as &$model )
        {
            array_push( self::isNew( $model ) ? $listInsert : $listUpdate, $model );
        }
    }

    /**
     * @param ModelAbstract $model
     * @return bool
     */
    public static function insert( ModelAbstract $model )
    {
        $columnNames    = array();
        $columnValues   = array();
        $modelClass     = \get_class( $model );

        foreach( $model->tableReflection()->columns as $column )
        {
            /* @var $column \Sql\Reflection\Column */

            // skip serial
            if( preg_match( '/^SERIAL[0-9]*$/i', $column->datatype ) && null == $model->{ $column->name } ) continue;

            $columnNames[] = $column->name;

            $columnValues[ ':' . $column->name ] = self::castToSql( $model->{$column->name}, $column->datatype );
        };

        $sql = 'INSERT INTO ' . self::getModelTable( $modelClass )
            . '(' . \implode( ',', $columnNames )
            . ') VALUES (' . \implode( ',', array_keys( $columnValues ) )
            . ') RETURNING ' . \implode( ',', (array)$modelClass::identityProperty() ) . ';';

        //echo $sql;

        $stmt = \SqlDb::factory()->prepare( $sql );

        $stmt->execute( $columnValues );

        $result = $stmt->fetch( \PDO::FETCH_ASSOC );

        foreach( $result as $column => $value )
        {
            $model->$column = $value;
        }

        return true;
    }

    /**
     * @param ModelAbstract[] $modelList
     * @throws \Erum\Exception
     * @return bool
     */
    public static function insertList( array $modelList )
    {
        if( empty( $modelList ) ) return 0;

        // if somehow we need to save that huge list - split it
        if( count( $modelList ) > 1000 )
        {
            $inserted   = 0;
            $iterate    = ceil( count( $modelList ) / 1000 );

            for( $i = 0; $i < $iterate; $i++ )
            {
                $inserted += static::insertList( array_slice( $modelList, $i * 1000, 1000 ) );
            }

            return $inserted;
        }

        $modelClass     = self::getModelClass();
        $dummyModel     = new $modelClass; // @TODO really ?!! That bad ?
        $columnList     = array();
        $columnValues   = array();
        $insertList     = array();

        foreach( $dummyModel->tableReflection()->columns as $column )
        {
            /* @var $column \Sql\Reflection\Column */

            $columnList[ $column->name ] = $column->datatype;
        }

        foreach( $modelList as &$model )
        {
            if( !($model instanceof $modelClass) ) throw new Exception('All models in collection must be single class instance.');

            $insertString = '';

            foreach( $columnList as $colName => $colType )
            {
                /* @var $column \Sql\Reflection\Column */

                // skip serial
                if( preg_match( '/^SERIAL[0-9]*$/i', $colType ) && null == $model->{$colName} )
                {
                    $insertString .= 'DEFAULT';
                }
                else
                {
                    $placeholder = ':' . $colName . count( $insertList );

                    $insertString .= $placeholder;

                    $columnValues[ $placeholder ] = self::castToSql( $model->{$colName}, $colType );
                }

                $insertString .= ',';
            }

            $insertList[] = '(' . trim( $insertString, ',' ) . ')';
        }

        $sql = 'INSERT INTO ' . self::getModelTable( $modelClass )
            . '(' . \implode( ',', array_keys( $columnList ) )
            . ') VALUES ' . \implode( ',', $insertList ) . '  '
            . 'RETURNING ' . \implode( ',', (array)$modelClass::identityProperty() ) . ';';

        $stmt = \SqlDb::factory()->prepare( $sql );

        $stmt->execute( $columnValues );

        $result = $stmt->fetch( \PDO::FETCH_ASSOC );

        foreach( $result as $column => $value )
        {
            $model->$column = $value;
        }

        return $stmt->rowCount();
    }


    /**
     * @param ModelAbstract[] $modelList
     * @throws \Erum\Exception
     * @return bool
     */
    public static function updateList( array $modelList )
    {
        if( empty( $modelList ) ) return 0;

        foreach( $modelList as &$model ) static::update( $model );

        return count( $modelList );
    }


    /**
     * @param ModelAbstract $model
     *
     * @return bool
     */
    public static function update( ModelAbstract $model )
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

        return true;
    }

    /**
     * Find out - is model new (need insert) or existed (need update)
     *
     * @param ModelAbstract $model
     * @return bool
     */
    public static function isNew( ModelAbstract $model )
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
     * @throws \Exception
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

    /**
     * @TODO refactor
     *
     * @param $value
     * @param string $columnType
     * @return int|null|string
     */
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
            case 'VARCHAR':
            case 'TEXT':
                $value = (string)$value;
                if( !strlen( $value ) ) $value = null;
                break;
            case 'INT':
            case 'SMALLINT':
            case 'BIGINT':
            case 'INTEGER':
                $value = (int)$value;
                break;
            case 'BOOLEAN':
            case 'BOOL':
                $value = (bool)$value ? 'TRUE': 'FALSE';
                break;
            case 'ARRAY':
                $value = self::arr2pgarr( $value );
                break;
            case 'POINT':
                $value = implode(',', (array)$value );
                break;
            case 'JSON':
                $value = null === $value ? null : json_encode( $value );
                break;
            case 'TIME':
                // 04:05:06
                if( !($value instanceof \DateTime ) )
                {
                    $value = new \DateTime( $value );
                }

                $value = $value->format( 'H:i:sO' );
                break;
            case 'TIMESTAMP':
            case 'TIMESTAMPTZ':
                // 2004-10-19 10:23:54
                if( !($value instanceof \DateTime ) )
                {
                    $value = new \DateTime( $value );
                }

                $value = $value->format( \DateTime::ISO8601 );
                break;
            case 'NUMRANGE':
            case 'INT8RANGE':
            case 'INT4RANGE':
                $value = (array)$value;

                if( !empty( $value ) )
                {
                    $value = '[' . $value[0] . ',' . $value[ sizeof($value) - 1 ] . ']';
                }
                else
                {
                    $value = null;
                }

                break;
            case 'GEOMETRY':
            case 'GEOM':
                // convert to binary from geoPHP object
                if( class_exists('geoPHP') && $value instanceof \Geometry )
                {
                    $unpack = unpack( 'H*', $value->out('ewkb') );
                    $value  = $unpack[1];
                    unset( $unpack );
                }
                break;
            default:
        }

        return $value;
    }

    /**
     * @TODO refactor
     *
     * @param $value
     * @param string $columnType
     * @return array|bool|\DateTime|int|mixed|null
     */
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
            case 'POINT':
                $value = array_map( 'floatval', explode( ',', trim( $value, ' ()') ) );
                break;
            case 'JSON':
                $value = null === $value ? null : json_decode( $value, true );
                break;
            case 'TIME':
            case 'TIMESTAMPTZ':
                // 04:05:06
                $value = new \DateTime( $value );
                break;
            case 'TIMESTAMP':
                $value = new \DateTime( $value, new \DateTimeZone('UTC') );
                break;
            // support only integer ranges for now
            //case 'NUMRANGE':
            //case 'INT8RANGE':
            case 'INT4RANGE':
                // remove inclusive bounds
                $value = explode( ',', trim( $value, '[]') );

                // test tor exclusive bounds
                $value[0] = $value[0]{0} == '(' ? $value[0] == (int)$value[0] + 1 : (int)$value[0];
                $value[1] = substr( $value[1], -1 ) == ')' ? (int)$value[1] - 1 : (int)$value[1];

                break;
            case 'GEOMETRY':
            case 'GEOM':
                // convert to geoPHp from binary if lib available
                if( class_exists('geoPHP') )
                {
                    $wkb    = pack('H*', $value);
                    $value  = \geoPHP::load( $wkb, 'ewkb' );
                }
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
            // for unknown reasons '+' at string start not cause string detection
            elseif( is_numeric( $inner ) && substr( $inner, 0, 1 ) !== '+' )
            {
                $parts[] = (float)$inner;
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
