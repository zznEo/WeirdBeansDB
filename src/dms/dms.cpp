#include "dms.hpp"
#include "pd.hpp"

using namespace bson;

const char *gKeyFieldName=DMS_KEY_FIELDNAME;


dmsFile::dmsFile(ixmBucketManager *ixmBucketMgr):_header(NULL),_pFileName(NULL)
{
   _ixmBucketMgr=ixmBucketMgr;
}

dmsFile::~dmsFile()
{
    if(_pFileName)  free(_pFileName);
    close();    //mmap的各种段解映射1�7
}

//插入流程
//1.棢�查record大小及是否包含_id
//2.棢�查获取数据库锁，棢�查有无符合记录大小的顄1�7
//3.没有则释放数据库锁，尝试获得扩展锁来扩展segment
//4.棢�查页内可用空间是否足够，如果不够进行重组
//5.棢�查可用空间是否存的下记录，存不下报错
//6.更新slot
//7.将记录拷贝进顄1�7
//8.更新记录头信恄1�7
//9.更新页头信息
//10.更新数据库信恄1�7
//11.释放数据库锁
int dmsFile::insert(BSONObj &record,BSONObj &outRecord,dmsRecordID &rid)
{
   int rc                     = EDB_OK;
   PAGEID pageID              = 0;
   char *page                 = NULL;
   dmsPageHeader *pageHeader  = NULL;
   int recordSize             = 0;
   SLOTOFF offsetTemp         = 0;
   const char *pGKeyFieldName = NULL;
   dmsRecord recordHeader;

   //1. 棢�查record大小
   recordSize=record.objsize();
   if((unsigned int)recordSize>DMS_MAX_RECORD)
   {
      rc = EDB_INVALIDARG;
      PD_LOG ( PDERROR, "record cannot bigger than 4MB" );
      goto error;
   }  
   pGKeyFieldName=gKeyFieldName;
   //getFieldDottedOrArray返回指定域名中的内容，意思是棢�查record是否带有_id
   if(record.getFieldDottedOrArray(pGKeyFieldName).eoo())
   {
      rc = EDB_INVALIDARG;
      PD_LOG ( PDERROR, "record must be with _id" );
      goto error;
   }


retry:
   _mutex.get();  //2.棢�查获取数据库锁，棢�查有无符合记录大小的顄1�7
   pageID=_findPage(recordSize+sizeof(dmsRecord));
   if(DMS_INVALID_PAGEID==pageID)
   {
      _mutex.release();//3.没有则释放数据库锁，尝试获得扩展锁来扩展segment
      if(_extendMutex.try_get())//尝试得到扩展锄1�7
      {
         rc=_extendSegment();//得到了则扩展殄1�7
         if(rc)
         {
            PD_LOG ( PDERROR, "Failed to extend segment, rc = %d", rc );
            _extendMutex.release ();
            goto error;
         }
      }
      else
      {
         _extendMutex.get();//如果没能得到，说明其它线程正在扩展，等待扩展完后得到锁��1�7
      }
      //此时已经得到了扩展锁，将其释放即可��1�7
      _extendMutex.release();
      goto retry;//重新棢�查有无可用页
   }
//4.棢�查页内可用空间是否足够，如果不够进行重组
   page=pageToOffset(pageID);
   if(!page)//根据pageid找到顄1�7
   {
      rc = EDB_SYS;
      PD_LOG ( PDERROR, "Failed to find the page" );
      goto error_releasemutex;
   }

   pageHeader=(dmsPageHeader*)page;//赋予页头
   //先检查标识是否为顄1�7
   if(memcmp(pageHeader->_eyeCatcher,DMS_PAGE_EYECATCHER,DMS_PAGE_EYECATCHER_LEN)!=0)
   {
      rc = EDB_SYS;
      PD_LOG ( PDERROR, "Invalid page header" );
      goto error_releasemutex;
   }

   //5.棢�查可用空间是否存的下记录，存不下报错
   //当可用空间大于可用空间偏秄1�7-slot朢�后的偏移，也就是实际可用空间
   //并且当slot朢�后偏秄1�7+记录长度+记录头长庄1�7+slot长度大于了可用空间偏秄1�7
   //意味睢�空间不够了，我们尝试重组
   if((pageHeader->_freeSpace>pageHeader->_freeOffset-pageHeader->_slotOffset)&&
      (pageHeader->_slotOffset+recordSize+sizeof(dmsRecord)+sizeof(SLOTID)>pageHeader->_freeOffset))
      {
         _recoverSpace(page);
      }

   //如果记录长度+记录头长庄1�7+slot长度大于了可用空闄1�7 或��1�7
   //页可用空间偏秄1�7-页slot朢�后偏移小亄1�7+记录长度+记录头长庄1�7+slot长度
   //都意味着可用空间还是不够，此时我们没有办法只能报锄1�7
   if(pageHeader->_freeSpace<recordSize+sizeof(dmsRecord)+sizeof(SLOTID)||
      pageHeader->_freeOffset-pageHeader->_slotOffset<recordSize+sizeof(dmsRecord)+sizeof(SLOTID))
   {
      PD_LOG ( PDERROR, "Something big wrong!!" );
      rc = EDB_SYS;
      goto error_releasemutex;
   }
   //注意上述的比较，丢�个是基于空间的，丢�个是基于偏移的1�7

   //此时我们把当前记录所在的偏移记录下来，并修改记录的头数据
   offsetTemp=pageHeader->_freeOffset-recordSize-sizeof(dmsRecord);
   recordHeader._size=recordSize+sizeof(dmsRecord);
   recordHeader._flag=DMS_RECORD_FLAG_NORMAL;

   //6.更新slot
   *(SLOTOFF*)(page+sizeof(dmsPageHeader)+pageHeader->_numSlots*sizeof(SLOTOFF))=offsetTemp;

   //7.将记录拷贝进顄1�7
   memcpy(page+offsetTemp,(char*)&recordHeader,sizeof(dmsRecord));
   memcpy(page+offsetTemp+sizeof(dmsRecord),record.objdata(),recordSize);
   outRecord=BSONObj(page+offsetTemp+sizeof(dmsRecord));//将记录传凄1�7

   //8.更新记录头信恄1�7
   rid._pageID=pageID;
   rid._slotID=pageHeader->_numSlots;

   //9.更新页头信息
   pageHeader->_numSlots++;
   pageHeader->_slotOffset+=sizeof(SLOTID);
   pageHeader->_freeOffset=offsetTemp;

   //10.更新数据库信恄1�7
   //注意第二个形参，正��代表释放空间，负��代表占用空闄1�7
   _updateFreeSpace(pageHeader,-(recordSize+sizeof(SLOTID)+sizeof(dmsRecord)),pageID);

   //11.释放空间锄1�7

   _mutex.release();
done :
   return rc;
error_releasemutex :
   _mutex.release();
error :
   goto done;
}

//删除过程
//1.依靠record id寻找对应的page
//2.依靠page地址，slotid寻找到对应的slot
//3.将slot值空，更新record头信息，更新数据库信恄1�7
int dmsFile::remove(dmsRecordID &rid) {
   int rc                    = EDB_OK;
   SLOTOFF slot              = 0;
   char *page                = NULL;
   dmsRecord *recordHeader   = NULL;
   dmsPageHeader *pageHeader = NULL;
   std::pair<std::multimap<unsigned int, PAGEID>::iterator, std::multimap<unsigned int, PAGEID>::iterator> ret;
   _mutex.get();

   page = pageToOffset(rid._pageID);
   if (!page) {
      rc = EDB_SYS;
      PD_LOG(PDERROR, "Failed to find the apge for %u;%u", rid._pageID, rid._slotID);
      goto error;
   }

   rc = _searchSlot(page, rid, slot);
   if (rc) {
      PD_LOG(PDERROR, "Failed to search slot, rc = %d", rc);
      goto error;
   }
   if (slot == DMS_SLOT_EMPTY) {
      rc = EDB_SYS;
      PD_LOG(PDERROR, "The record is dropped");
      goto error;
   }

   pageHeader = (dmsPageHeader*)page;
   *(SLOTID*)(page + sizeof(dmsPageHeader) + rid._slotID*sizeof(SLOTID)) = DMS_SLOT_EMPTY;
   recordHeader = (dmsRecord*)(page + slot);
   recordHeader->_flag = DMS_RECORD_FLAG_DROPPED;
   _updateFreeSpace(pageHeader, recordHeader->_size,rid._pageID);
done:
   _mutex.release();
   return rc;
error:
   goto done;
}

//寻找过程 与删除过程类似，多了丢�个步骤需要将记录传出
//1.根据recordid找到对应顄1�7
//2.根据page和slotid找到slot
//3.找到record复制给result
int dmsFile::find(dmsRecordID &rid,BSONObj &result)
{
   int rc                  = EDB_OK;
   SLOTOFF slot            = 0;
   char *page              = NULL;
   dmsRecord *recordHeader = NULL;

   _mutex.get_shared();

   //1.根据recordid找到对应顄1�7
   page = pageToOffset ( rid._pageID );
   if ( !page )
   {
      rc = EDB_SYS;
      PD_LOG ( PDERROR, "Failed to find the page" );
      goto error;
   }

   //2.根据page和slotid找到slot
    rc = _searchSlot ( page, rid, slot );
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to search slot, rc = %d", rc );
      goto error;
   }
   
   if ( DMS_SLOT_EMPTY == slot )
   {
      rc = EDB_SYS;
      PD_LOG ( PDERROR, "The record is dropped" );
      goto error;
   }

   //3.找到record复制给result
   
   recordHeader = (dmsRecord *)( page + slot );
   
   if ( DMS_RECORD_FLAG_DROPPED == recordHeader->_flag )
   {
      rc = EDB_SYS;
      PD_LOG ( PDERROR, "This data is dropped" );
      goto error;
   }
   //虽然没有标明要复制多长，但是BSONObj本身会记录长庄1�7
   result = BSONObj ( page + slot + sizeof(dmsRecord) ).copy ();
done :
   _mutex.release_shared ();
   return rc;
error :
   goto done;
}

//将changeSize指定大小的页更新到freespace里（仅仅是映射更新）
void dmsFile::_updateFreeSpace(dmsPageHeader *header,
                               int changeSize,PAGEID pageID)
{   //首先根据给定的header获取霢�要page的freespace
    unsigned int freeSpace=header->_freeSpace;
    //pair保存了_freespacemap类型的迭代器
    std::pair<std::multimap<unsigned int,PAGEID>::iterator,
              std::multimap<unsigned int,PAGEID>::iterator> ret;
    //根据freeSpace的大小，在_freeSpaceMap中寻找符合key值的迭代器范囄1�7
    ret=_freeSpaceMap.equal_range(freeSpace);

    //起点为pair中PAGEID朢�小的迭代器first，终点为PAGEID朢�大的迭代器second
    for(std::multimap<unsigned int,PAGEID>::iterator it=ret.first;
        it!=ret.second;++it)
    {
        if(it->second==pageID)  //如果PAGEID和形参给定一臄1�7
        {   //我们先将这个映射抹去
            _freeSpaceMap.erase(it);
            break;
        }
    }

    freeSpace+=changeSize;
    header->_freeSpace=freeSpace;
    //将修改过的freespace重新加入映射
    _freeSpaceMap.insert(pair<unsigned int,PAGEID>(freeSpace,pageID));

}

int dmsFile::initialize(const char *pFileName)
{
    offsetType offset=0;
    int rc=EDB_OK;

    _pFileName=strdup(pFileName);   //复制文件各1�7
    if ( !_pFileName )
    {
      rc = EDB_OOM;
      PD_LOG ( PDERROR, "Failed to duplicate file name" );
      goto error;
    }

    //打开文件
    rc=open(_pFileName,OSS_PRIMITIVE_FILE_OP_OPEN_ALWAYS);
    PD_RC_CHECK ( rc, PDERROR, "Failed to open file %s, rc = %d",
                 _pFileName, rc );
        
getfilesize:
    rc=_fileOp.getSize(&offset);//获得文件大小
    PD_RC_CHECK ( rc, PDERROR, "Failed to get file size, rc = %d",
                 rc ); 

    if(!offset) //获得文件大小失败
    {
        rc=_initNew();  //重新初始化文仄1�7
        PD_RC_CHECK ( rc, PDERROR, "Failed to initialize file, rc = %d",
                    rc );
        goto getfilesize;  //再去获得文件大小
    }

    rc=_loadData(); //装载数据
    PD_RC_CHECK ( rc, PDERROR, "Failed to load data, rc = %d", rc );
done :
   return rc;
error :
   goto done;
}

int dmsFile::_extendSegment()
{
    int rc=EDB_OK;
    char *data=NULL;
    int freeMapSize=0;
    dmsPageHeader pageHeader;
    offsetType offset=0;

    //让我们先获得文件已经扩张的大射1�7
    rc=_fileOp.getSize(&offset);
    PD_RC_CHECK ( rc, PDERROR, "Failed to get file size, rc = %d", rc );

    rc=_extendFile(DMS_FILE_SEGMENT_SIZE);//增加128M
    PD_RC_CHECK ( rc, PDERROR, "Failed to extend segment rc = %d", rc );

    //从原来的end到新的end这一段映射过厄1�7
    //调用mmap进行内存映射，data为内存映射的指针
    rc=map(offset,DMS_FILE_SEGMENT_SIZE,(void**)&data);
    PD_RC_CHECK ( rc, PDERROR, "Failed to map file, rc = %d", rc );

    //接下来我们要将段中每丢�个页初始匄1�7
    //自然我们先把页的头给定义奄1�7
    strcpy(pageHeader._eyeCatcher,DMS_PAGE_EYECATCHER);
    pageHeader._size = DMS_PAGESIZE;
    pageHeader._flag = DMS_PAGE_FLAG_NORMAL;
    pageHeader._numSlots = 0;
    pageHeader._slotOffset = sizeof ( dmsPageHeader );
    pageHeader._freeSpace = DMS_PAGESIZE - sizeof(dmsPageHeader);
    pageHeader._freeOffset = DMS_PAGESIZE;

    //把页头给每一页补丄1�7
    for(int i=0;i<DMS_FILE_SEGMENT_SIZE;i+=DMS_PAGESIZE)//丢�兄1�7128M，每次增劄1�74M
    {
        memcpy(data+i,(char*)&pageHeader,sizeof(dmsPageHeader));
    }

    //页的空闲空间和PAGEID的映射更斄1�7
    _mutex.get();
    freeMapSize=_freeSpaceMap.size();//为了获取映射PAGEID现在到哪亄1�7
    for(int i=0;i<DMS_PAGES_PER_SEGMENT;++i)
    {
        _freeSpaceMap.insert(pair<unsigned int,PAGEID>(pageHeader._freeSpace,i+freeMapSize));
    }

    //将新的segment放到Body丄1�7
    _body.push_back(data);
    _header->_size+=DMS_PAGES_PER_SEGMENT;
    _mutex.release();
done:
    return rc;
error:
    goto done;
}

//initialize发现size丄1�70时进衄1�7
int dmsFile::_initNew()
{
    int rc=EDB_OK;
    rc=_extendFile(DMS_FILE_HEADER_SIZE);       //先扩展文件头郄1�7
    PD_RC_CHECK ( rc, PDERROR, "Failed to extend file, rc = %d", rc );
    rc = map ( 0, DMS_FILE_HEADER_SIZE, ( void **)&_header );  //头部映射到内孄1�7
    PD_RC_CHECK ( rc, PDERROR, "Failed to map, rc = %d", rc );

    //初始头部信息
    strcpy ( _header->_eyeCatcher, DMS_HEADER_EYECATCHER );
   _header->_size = 0;
   _header->_flag = DMS_HEADER_FLAG_NORMAL;
   _header->_version = DMS_HEADER_VERSION_CURRENT;

done :
   return rc;
error :
   goto done;
}

//根据霢�要的空间大小，在空闲空间的map中找到最合��的PAGE
PAGEID dmsFile::_findPage(size_t requiredSize)
{
    std::multimap<unsigned int,PAGEID>::iterator findIter;
    findIter=_freeSpaceMap.upper_bound(requiredSize);   //找到Map中比requiredSize刚好大一点的迭代噄1�7

    if ( findIter != _freeSpaceMap.end() )
    {
      return findIter->second;
    }
    return DMS_INVALID_PAGEID;
}

//先将文件分配的区域置穄1�7
int dmsFile::_extendFile(int size)
{
    int rc=EDB_OK;
    char temp[DMS_EXTEND_SIZE]={0}; //先定乄1�764K的文件头大小
    memset(temp,0,DMS_EXTEND_SIZE);

    //如果size不是64K的��数，那么是无效的输兄1�7
    if ( size % DMS_EXTEND_SIZE != 0 )
    {
      rc = EDB_SYS;
      PD_LOG ( PDERROR, "Invalid extend size, must be multiple of %d",
               DMS_EXTEND_SIZE );
      goto error;
    }

    //将分配的空间置空，每次置穄1�764K
    for ( int i = 0; i < size; i += DMS_EXTEND_SIZE )
    {
      _fileOp.seekToEnd ();
      rc = _fileOp.Write ( temp, DMS_EXTEND_SIZE );
      PD_RC_CHECK ( rc, PDERROR, "Failed to write to file, rc = %d", rc );
    }
done :
   return rc;
error :
   goto done;
}

//根据给定的page地址和记录ID(pageid slotid)找到slot的偏秄1�7
int dmsFile::_searchSlot(char *page,dmsRecordID &rid,SLOTOFF &slot)
{
   int rc = EDB_OK;
   dmsPageHeader *pageHeader = NULL;
   //棢�查page地址
   if ( !page )
   {
      rc = EDB_SYS;
      PD_LOG ( PDERROR, "page is NULL" );
      goto 
      
      error;
   }

    //棢�查RID是否合法
   if ( 0 > rid._pageID || 0 > rid._slotID )
   {
      rc = EDB_SYS;
      PD_LOG ( PDERROR, "Invalid RID: %d.%d",
               rid._pageID, rid._slotID );
      goto error;
   }

   pageHeader = (dmsPageHeader *)page; //得到page地址
   //棢�查slotID是否合法
   if ( rid._slotID > pageHeader->_numSlots )
   {
      rc = EDB_SYS;
      PD_LOG ( PDERROR, "Slot is out of range, provided: %d, max: %d",
               rid._slotID, pageHeader->_numSlots );
      goto error;
   }

    //slot的偏移为 当前页地坢�+页头+slotID*slot大小
   slot=*(SLOTOFF*)(page+sizeof(dmsPageHeader)+rid._slotID*sizeof(SLOTOFF));
done :
   return rc;
error :
   goto done;
}

//装载数据，将文件头，各个sements映射到内存，并将_body,_freeSpaceMap，index更新
//注意文件头，页头等内容已经存在了
int dmsFile::_loadData()
{
    int rc=EDB_OK;
    int numPage=0;
    int numSegments=0;
    dmsPageHeader *pageHeader=NULL;
    char *data=NULL;
    BSONObj bson;
    SLOTID slotID=0;
    SLOTOFF slotOffset=0;
    dmsRecordID recordID;

    //棢�查文件头是否存在
    if(!_header)
    {
        rc = map ( 0, DMS_FILE_HEADER_SIZE, ( void **)&_header );
        PD_RC_CHECK ( rc, PDERROR, "Failed to map file header, rc = %d", rc );
    }

    numPage=_header->_size;
    if ( numPage % DMS_PAGES_PER_SEGMENT )//棢�查页数是否为segment倍数
    {
         rc = EDB_SYS;
         PD_LOG ( PDERROR, "Failed to load data, partial segments detected" );
         goto error;
    }

    numSegments=numPage/DMS_PAGES_PER_SEGMENT;  //获得段数
    if ( numSegments > 0 )
    {
      for ( int i = 0; i < numSegments; ++i )   //
      {
         // 将已经存在的段映射到内存丄1�7
         rc = map ( DMS_FILE_HEADER_SIZE + DMS_FILE_SEGMENT_SIZE * i,
                    DMS_FILE_SEGMENT_SIZE,
                    (void **)&data );
         PD_RC_CHECK ( rc, PDERROR, "Failed to map segment %d, rc = %d",
                       i, rc );
         _body.push_back ( data );//更新body
         // 将每页映射到freeSpaceMap
         for ( unsigned int k = 0; k < DMS_PAGES_PER_SEGMENT; ++k )
         {//获得每页的头
            pageHeader = ( dmsPageHeader * ) ( data + k * DMS_PAGESIZE );
            _freeSpaceMap.insert (
                  pair<unsigned int, PAGEID>(pageHeader->_freeSpace, k ) );
            slotID = ( SLOTID ) pageHeader->_numSlots;
            recordID._pageID = (PAGEID) k;
            
            //将每个记录都建立对应的索引放到内孄1�7
            for ( unsigned int s = 0; s < slotID; ++s )
            {  //获得record偏移
               slotOffset = *(SLOTOFF*)(data+k*DMS_PAGESIZE +
                            sizeof(dmsPageHeader ) + s*sizeof(SLOTID) );
               if ( DMS_SLOT_EMPTY == slotOffset )
               {
                  continue;
               }
               //获得record内容
               bson = BSONObj ( data + k*DMS_PAGESIZE +
                                slotOffset + sizeof(dmsRecord) );
               recordID._slotID = (SLOTID)s;
               //棢�查索引是否已经存圄1�7
               rc = _ixmBucketMgr->isIDExist ( bson );
               PD_RC_CHECK ( rc, PDERROR, "Failed to call isIDExist, rc = %d", rc );
               //根据bson对象和recordID建立索引
               rc = _ixmBucketMgr->createIndex ( bson, recordID );
               PD_RC_CHECK ( rc, PDERROR, "Failed to call ixm createIndex, rc = %d", rc );
            }
            
         }
      } // for ( int i = 0; i < numSegments; ++i )
   } // if ( numSegments > 0 )
done :
   return rc;
error :
   goto done;

}

//根据每个slot标记，对丢�个页里的无用记录进行整理
void dmsFile::_recoverSpace(char *page)
{
   char *pLeft               = NULL;
   char *pRight              = NULL;
   SLOTOFF slot              = 0;
   int recordSize            = 0;
   bool isRecover            = false;  //是否霢�要重组1�7
   dmsRecord *recordHeader   = NULL;
   dmsPageHeader *pageHeader = NULL;

   //pLeft先到该页的头之后，也就是第一个slot的位罄1�7
   pLeft=page+sizeof(dmsPageHeader);
   //pRight直接到该页的尾部
   pRight=page+DMS_PAGESIZE;
   pageHeader=(dmsPageHeader*)page;//获得该页的头位置

   for ( unsigned int i = 0; i < pageHeader->_numSlots; ++i )
   {    //获得该slot对应的记录偏秄1�7
      slot = *((SLOTOFF*)(pLeft + sizeof(SLOTOFF) * i ) );
      if ( DMS_SLOT_EMPTY != slot ) //slot标记不为-1，数据有甄1�7
      {
         recordHeader = (dmsRecord *)(page + slot );
         recordSize = recordHeader->_size;//获取记录长度
         pRight -= recordSize;
         if ( isRecover )   //注意如果没有过标记为-1的slot，是不需要进行move的，只需要移动pRight指针
         {//只要出现了标记为-1的slot，后续所有的有效记录都要弢�始move
            memmove ( pRight, page + slot, recordSize );
            //更新slot对应的记录偏秄1�7
            *((SLOTOFF*)(pLeft + sizeof(SLOTOFF) * i ) ) = (SLOTOFF)(pRight-page);
         }
      }
      else//slot标记丄1�7-1，该记录霢�要使得该页进行重组，后续都要弢�始move
      {
         isRecover = true;
      }
   }
   //将页头的空闲偏移更新
   pageHeader->_freeOffset = pRight - page;


}