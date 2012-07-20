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

        $query = 'SELECT ' . self::getModelTable() . '.* FROM ' . self::getModelTable() . ($condition ? ' ' . $condition : '');

        $stmt = \SqlDb::factory()->prepare( $query );
        
        $stmt->execute();

        $list = array( );

        while ( $model = $stmt->fetchObject( self::getModelClass( true ) ) )
        {
            $list[ implode( ':', (array)$model->id ) ] = $model;
            
            $oldModel = \Erum\ModelWatcher::instance()->get( self::getModelClass( true ), $model->id );
            
            if( $model )
            {
                unset( $model );
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
    
}
