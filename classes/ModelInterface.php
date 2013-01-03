<?php
namespace SqlDb;

interface ModelInterface
{
    /**
     * Table name
     */
    const table = '';

    /**
     * Method must provide access to model table reflection
     *
     * @return \Sql\Reflection\Table
     */
    public function tableReflection();
}