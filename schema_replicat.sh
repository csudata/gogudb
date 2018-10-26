#!/bin/sh
host="localhost"
port=5432
user=`whoami`
password=""
database="postgres"
target_schema=""
target_table=""
output_script="./schema.sql"
get_tuple_from_table_partition_rule="select schema_name, table_name from _gogu.table_partition_rule "
usage()
{
  cat <<EOF
Usage: $0 [OPTIONS]
  --host=localhost	default localhost
  --port=5432		default 5432
  --database=postgres	default postgres
  --user=CURRENT_USER	default current user
  --password		default NULL
  --target_schema	default empty
  --target_table	default empty
  --output_script	default './schema.sql'
EOF
  exit 1
}

parse_arg()
{
  echo "$1" | sed -e 's/^[^=]*=//'
}

parse_arguments()
{
  for arg
  do
    case "$arg" in
      --host=*) host=`parse_arg "$arg"` ;;
      --port=*) port=`parse_arg "$arg"` ;;
      --database=*) database=`parse_arg "$arg"` ;;
      --user=*)  user=`parse_arg "$arg"` ;;
      --password=*) password=`parse_arg "$arg"` ;;
      --target_schema=*)target_schema=`parse_arg "$arg"` ;;
      --target_table=*)target_table=`parse_arg "$arg"` ;;
      --output_script=*)output_script=`parse_arg "$arg"` ;;
      --help) usage ;;
      *) usage ;;
    esac
  done
}

parse_arguments "$@"

if test -n "${target_schema}"
then
  if test -n "${target_table}"
  then
    get_tuple_from_table_partition_rule=${get_tuple_from_table_partition_rule}" where schema_name='${target_schema}' and table_name='${target_table}'"
  else
    get_tuple_from_table_partition_rule=${get_tuple_from_table_partition_rule}" where schema_name='${target_schema}'"
  fi
else 
  if test -n "${target_table}"
  then
    get_tuple_from_table_partition_rule=${get_tuple_from_table_partition_rule}" where table_name='${target_table}'"
  fi
fi

echo ${get_tuple_from_table_partition_rule}

psql -h ${host} -p ${port} -d ${database} -U ${user} --tuples-only -c "${get_tuple_from_table_partition_rule}" > /tmp/target.xxx

content=`cat /tmp/target.xxx |awk -F '|' '{printf("%s %s",$1,$2)}'`

# read -a schema_table </tmp/target.xxx
schema_table=(${content})
#echo ${schema_table[*]}
count=${#schema_table[*]}/2
echo "SET gogudb.enable = f;" > ${output_script}
for (( i=0; i<count; i++))
  do
    #echo ${schema_table[2*i]} ${schema_table[2*i+1]}
   sql="SELECT partition FROM _gogu.gogudb_partition_list WHERE parent = '${schema_table[2*i]}.${schema_table[2*i+1]}'::REGCLASS"
   psql -h ${host} -p ${port} -d ${database} -U ${user} --tuples-only -c "${sql}" > /tmp/partitions.xxx 2>/dev/null
   if [ $? -eq 0 ]
   then
     #read -a parts </tmp/partitions.xxx
     parts=`cat /tmp/partitions.xxx`
     dmpcmd="pg_dump -d ${database} -s -t ${schema_table[2*i]}.${schema_table[2*i+1]}"
     for part in ${parts[@]}
       do
         dmpcmd=${dmpcmd}" -t ${part}"
       done

     echo ${dmpcmd}
     `${dmpcmd} >> ${output_script}`

     pg_dump -d ${database} --insert  --table=_gogu.table_partition_rule | grep INSERT | grep ${schema_table[2*i+1]} >> ${output_script}
     pg_dump -d ${database} --insert  --table=_gogu.gogudb_config | grep INSERT | grep ${schema_table[2*i+1]} >> ${output_script}

   fi
   done

echo "SET gogudb.enable = t;" >> ${output_script}

