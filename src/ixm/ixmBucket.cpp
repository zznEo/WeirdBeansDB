#include    "core.hpp"
#include    "pd.hpp"
#include    "ossHash.hpp"
#include    "ixmBucket.hpp"

//manager鐨勫嚱鏁版槸绫讳技鐨勶紝閮介渶瑕佸厛閫氳繃澶勭悊鍑芥暟寰楀埌瀵瑰簲妗跺彿random
//鐒跺悗浜ょ粰瀵瑰簲妗跺鐞�


//manager绾у埆
//棣栧厛澶勭悊record
//鐒跺悗浜ょ粰瀵瑰簲鐨勬《鍘诲垽鏂�
int ixmBucketManager::isIDExist(BSONObj &record)
{
   int rc               = EDB_OK;
   unsigned int hashNum = 0;
   unsigned int random  = 0;
   ixmEleHash eleHash;
   dmsRecordID recordID;

    //澶勭悊璁板綍鏄敱Manager瀹屾垚鐨�
   rc = _processData ( record, recordID, hashNum, eleHash, random );
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to process data, rc = %d", rc );
      goto error;
   }

   //浜ょ粰瀵瑰簲妗跺鐞�
   rc = _bucket[random]->isIDExist ( hashNum, eleHash );
   if ( rc )
   {
      PD_LOG ( PDERROR, "Failed to create index, rc = %d", rc );
      goto error;
   }
done :
   return rc;
error :
   goto done;
}

//manager绾у埆
//棣栧厛澶勭悊record
//鐒跺悗浜ょ粰瀵瑰簲鐨勬《鍘诲垱寤�
int ixmBucketManager::createIndex(BSONObj &record,dmsRecordID &recordID)
{
   int rc                = EDB_OK;
   unsigned int hashNum  = 0;
   unsigned int random   = 0;
   ixmEleHash eleHash;
   rc = _processData ( record, recordID, hashNum, eleHash, random );
   PD_RC_CHECK ( rc, PDERROR, "Failed to process data, rc = %d", rc );
   rc = _bucket[random]->createIndex ( hashNum, eleHash );
   PD_RC_CHECK ( rc, PDERROR, "Failed to create index, rc = %d", rc );
   recordID = eleHash.recordID;
done :
   return rc;
error :
   goto done;
}

//manager绾у埆
//棣栧厛澶勭悊record
//鐒跺悗浜ょ粰瀵瑰簲鐨勬《鍘诲鎵�
int ixmBucketManager::findIndex ( BSONObj &record, dmsRecordID &recordID )
{
   int rc                = EDB_OK;
   unsigned int hashNum  = 0;
   unsigned int random   = 0;
   ixmEleHash eleHash;
   rc = _processData ( record, recordID, hashNum, eleHash, random );
   PD_RC_CHECK ( rc, PDERROR, "Failed to process data, rc = %d", rc );
   rc = _bucket[random]->findIndex ( hashNum, eleHash );
   PD_RC_CHECK ( rc, PDERROR, "Failed to find index, rc = %d", rc );
   recordID = eleHash.recordID;
done :
   return rc;
error :
   goto done;
}

//manager绾у埆
//棣栧厛澶勭悊record
//鐒跺悗浜ょ粰瀵瑰簲鐨勬《鍘荤Щ闄�
int ixmBucketManager::removeIndex ( BSONObj &record, dmsRecordID &recordID )
{
   int rc                = EDB_OK;
   unsigned int hashNum  = 0;
   unsigned int random   = 0;
   ixmEleHash eleHash;
   rc = _processData ( record, recordID, hashNum, eleHash, random );
   PD_RC_CHECK ( rc, PDERROR, "Failed to process data, rc = %d", rc );
   rc = _bucket[random]->removeIndex ( hashNum, eleHash );
   PD_RC_CHECK ( rc, PDERROR, "Failed to remove index, rc = %d", rc );
   recordID._pageID = eleHash.recordID._pageID;
   recordID._slotID = eleHash.recordID._slotID;
done :
   return rc;
error :
   goto done;
}

//澶勭悊鏁版嵁璁板綍鍑芥暟
//鍏堟楠岃褰曟槸鍚︽湁_id瀛楁鎵撳ご
//鏍规嵁鏁ｅ垪鍊艰幏寰楁《鍙穜andom
//鎶婃暎鍒楀厓绱犵殑data鍜宨d闄勪笂鍊�

int ixmBucketManager::_processData (BSONObj &record, dmsRecordID &recordID, unsigned int &hashNum, ixmEleHash &eleHash, unsigned int &random ) {
   int rc               = EDB_OK;
   //寰楀埌瀛楁涓篿_id鐨凚SON鍏冪礌閮ㄥ垎
   BSONElement element  = record.getField (IXM_KEY_FIELDNAME);
   //濡傛灉娌℃湁_id瀛楁锛屾垨鑰卂id鐨勭被鍨嬩笉鏄痠nt鎴栬€卻tring 鍒欏嚭閿�
   if (element.eoo() || (element.type() != NumberInt && element.type() != String)) {
      rc = EDB_INVALIDARG;
      PD_LOG ( PDERROR, "record must be with _id" );
      goto error;
   }

   // 鏍规嵁_id鐨勫€煎拰闀垮害锛岀粡杩囨暎鍒楀嚱鏁拌幏寰楁暎鍒楀€�
   hashNum = ossHash(element.value(), element.valuesize());
   // 鏍规嵁鏁ｅ垪鍊奸€氳繃鍙栨ā寰楀埌妗跺彿
   random = hashNum % IXM_HASH_MAP_SIZE;

   //灏嗘暎鍒楀厓绱犺祴鍊�
   eleHash.data = element.rawdata ();
   eleHash.recordID = recordID;
done:
   return rc;
error:
   goto done;
}

//妗秏anager鐨勫垵濮嬪寲锛屽氨鏄皢vector涓殑妗朵竴涓釜鎻掑叆
int ixmBucketManager::initialize ()
{
   int rc = EDB_OK;
   ixmBucket *temp = NULL;
   for ( int i = 0; i < IXM_HASH_MAP_SIZE; ++i )
   {
      temp = new (std::nothrow) ixmBucket ();
      if ( !temp )
      {
         rc = EDB_OOM;
         PD_LOG ( PDERROR, "Failed to allocate new ixmBucket" );
         goto error;
      }
      _bucket.push_back ( temp );
      temp = NULL;
   }
done:
   return rc;
error :
   goto done;
}

//鍏蜂綋鐨勮褰曟鏌ュ嚱鏁帮細
//棣栧厛宸茬粡缁忚繃Manager鐨勮褰曞鐞嗗嚱鏁帮紝寰楀埌浜嗗搴旂殑鏁ｅ垪鍊煎拰鏁ｅ垪鍏冪礌
//鍘诲搴旂殑妗堕噷瀵绘壘鐩稿悓鏁ｅ垪鍊肩殑杩唬鍣ㄨ寖鍥�
//鍦ㄨ繖涓寖鍥村唴鍒ゆ柇鏄惁涓庢暎鍒楀厓绱犵浉鍚岋細鍏堝垽鏂暟鎹被鍨�->鏁版嵁闀垮害->鏁版嵁鏈韩
//鎴戜滑瀵硅妗朵娇鐢ㄥ叡浜攣

int ixmBucketManager::ixmBucket::isIDExist ( unsigned int hashNum,
                                             ixmEleHash &eleHash )
{
   int rc = EDB_OK;
   BSONElement destEle;
   BSONElement sourEle;
   ixmEleHash existEle;
   std::pair<std::multimap<unsigned int, ixmEleHash>::iterator,
             std::multimap<unsigned int, ixmEleHash>::iterator> ret;
   _mutex.get_shared ();
   ret = _bucketMap.equal_range ( hashNum );   //鐩稿悓鏁ｅ垪鍊肩殑杩唬鍣ㄨ寖鍥�
   sourEle = BSONElement ( eleHash.data );
   for ( std::multimap<unsigned int, ixmEleHash>::iterator it = ret.first;
         it != ret.second; ++it )
   {
      existEle = it->second;
      destEle = BSONElement ( existEle.data );
      if ( sourEle.type() == destEle.type() )   //鍏堝垽鏂€肩殑绫诲瀷鏄惁鐩稿悓
      {
         if ( sourEle.valuesize() == destEle.valuesize() )  //鍐嶅垽鏂暱搴�
         {
            if ( !memcmp ( sourEle.value(), destEle.value(),    //鏈€鍚庡垽鏂唴瀹�
                           destEle.valuesize() ) )
            {
               rc = EDB_IXM_ID_EXIST;
               PD_LOG ( PDERROR, "record _id does exist" );
               goto error;
            }
         }
      }
   }
done :
   _mutex.release_shared ();
   return rc;
error :
   goto done;
}

//鍏蜂綋鐨勮绱㈠紩鍒涘缓鍑芥暟锛�
//灏嗙粡杩囨暟鎹鐞嗗緱鍒扮殑鏁ｅ垪鍊煎拰鏁ｅ垪鍏冪礌鎻掑叆鍒癿ap涓嵆鍙€�
//浣跨敤鍐欓攣
int ixmBucketManager::ixmBucket::createIndex ( unsigned int hashNum,
                                               ixmEleHash &eleHash )
{
   int rc = EDB_OK;
   _mutex.get();
   _bucketMap.insert (
      pair<unsigned int, ixmEleHash> ( hashNum, eleHash ) );
   _mutex.release ();
   return rc;
}

//鍏蜂綋鐨勮褰曟煡鎵惧嚱鏁帮細
//杩囩▼涓庤褰曟鏌ュ嚱鏁拌褰曠浉鍚岋紝鍖哄埆鍦ㄤ簬瀵规瘮瀹岃褰曞畬鍏ㄧ浉鍚屼互鍚庯紝杩斿洖璁板綍ID
//鎴戜滑瀵硅妗朵娇鐢ㄥ叡浜攣


int ixmBucketManager::ixmBucket::findIndex (unsigned int hashNum, ixmEleHash &eleHash) {
   int rc = EDB_OK;
   BSONElement destEle;
   BSONElement sourEle;
   ixmEleHash existEle;
   std::pair<std::multimap<unsigned int, ixmEleHash>::iterator, std::multimap<unsigned int, ixmEleHash>::iterator> ret;
   _mutex.get_shared(); // 对桶加锁
   ret = _bucketMap.equal_range(hashNum);
   sourEle = BSONElement(eleHash.data);
   for (std::multimap<unsigned int, ixmEleHash>::iterator it = ret.first; it != ret.second; ++it) {
      existEle = it->second;
      destEle = BSONElement(existEle.data);
      if (sourEle.type() == destEle.type()) {  // 判断值类型是否相同
         if (sourEle.valuesize() == destEle.valuesize()) { // 判断值大小是否相同
            if (!memcmp (sourEle.value(), destEle.value(), destEle.valuesize())) { // 判断内容
               eleHash.recordID = existEle.recordID; // 返回真实查找到的信息
               goto done;
            }
         }
      }
   }
   rc = EDB_IXM_ID_NOT_EXIST;
   PD_LOG(PDERROR, "record _id does not exist, hashNum = %d", hashNum);
   goto error;
done:
   _mutex.release_shared();
   return rc;
error:
   goto done;
}

//鍏蜂綋鐨勮褰曟煡鎵惧嚱鏁帮細
//杩囩▼涓庤褰曟鏌ュ嚱鏁拌褰曠浉鍚岋紝鍖哄埆鍦ㄤ簬瀵规瘮瀹岃褰曞畬鍏ㄧ浉鍚屼互鍚庯紝map涓璭rase杩欐潯
//鎴戜滑瀵硅妗朵娇鐢ㄥ啓閿�

int ixmBucketManager::ixmBucket::removeIndex ( unsigned int hashNum,
                                             ixmEleHash &eleHash )
{
   int rc = EDB_OK;
   BSONElement destEle;
   BSONElement sourEle;
   ixmEleHash existEle;
   std::pair<std::multimap<unsigned int, ixmEleHash>::iterator,
             std::multimap<unsigned int, ixmEleHash>::iterator> ret;
   _mutex.get ();
   ret = _bucketMap.equal_range ( hashNum );
   sourEle = BSONElement ( eleHash.data );
   for ( std::multimap<unsigned int, ixmEleHash>::iterator it = ret.first;
         it != ret.second; ++it )
   {
      existEle = it->second;
      destEle = BSONElement ( existEle.data );
      if ( sourEle.type() == destEle.type() )
      {
         if ( sourEle.valuesize() == destEle.valuesize() )
         {
            if ( !memcmp ( sourEle.value(), destEle.value(),
                           destEle.valuesize() ) )
            {
               eleHash.recordID = existEle.recordID;
               _bucketMap.erase ( it );    //瀹屽叏鐩哥瓑鍚庯紝map涓殑瀵筫rase
               goto done;
            }
         }
      }
   }
   rc = EDB_INVALIDARG;
   PD_LOG ( PDERROR, "record _id does not exist" );
   goto error;
done :
   _mutex.release ();
   return rc;
error :
   goto done;
}