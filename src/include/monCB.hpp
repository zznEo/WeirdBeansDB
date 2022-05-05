/******************************************************************************
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.
*******************************************************************************/

#ifndef SNAPSHOT_HPP_
#define SNAPSHOT_HPP_

#include    <time.h>
#include    "ossLatch.hpp"

class MonAppCB {
public:
    MonAppCB();
    ~MonAppCB();
    void setInsertTimes(long long insertTimes);
    long long getInsertTimes()const;
    void increaseInsertTimes();
    void setDelTimes(long long delTimes);
    long long getDelTimes()const;
    void increaseDelTimes();
    void setQueryTimes(long long queryTimes);
    long long getQueryTimes()const;
    void increaseQueryTimes();
    long long getServerRunTime();
private:
    long long _insertTimes;
    long long _delTimes;
    long long _queryTimes;
    struct timeval _start;
    ossSLatch _mutex;
};

#endif 