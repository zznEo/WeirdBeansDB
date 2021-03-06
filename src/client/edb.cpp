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

#include <iostream>
#include <sstream>
#include "core.hpp"
#include "edb.hpp"
#include "command.hpp"

const char    SPACE             =  ' ';
const char    TAB               =  '\t';
const char    BACK_SLANT        =  '\\';
const char    NEW_LINE          =   '\n';

int gQuit = 0;

void Edb::start(void) {
    std::cout << "Welcome to WeirdBeansDB Shell!" << std::endl;
    std::cout << "wdb help for help, Ctrl+c or quit to exit" << std::endl;
    while(gQuit == 0) {
        prompt();
    }
}

void Edb::prompt(void) {
    int ret = EDB_OK;
    ret = readInput("wdb", 0);
    if (ret) {
        //printf("nothing to do\n");
        return ;
    }

    // Input string
    std::string textInput = _cmdBuffer;
    //printf("Input string is: %s", textInput);
    // Split the inputing sentence.
    std::vector<std::string> textVec;
    split(textInput, SPACE, textVec);
    int count = 0;
    std::string cmd = "";
    std::vector<std::string> optionVec;
    std::vector<std::string>::iterator iter = textVec.begin();
    // handle different command here.
    ICommand *pCmd = NULL;
    for(;iter != textVec.end();iter++) {
        std::string str = *iter;
        if (count == 0) {
            cmd = str;
            count++;
        } else {
            optionVec.push_back(str);
        }
    }
    pCmd = _cmdFactory.getCommandProcesser(cmd.c_str());
    if(pCmd != NULL) {
        pCmd->execute(_sock, optionVec);
    }
}

int Edb::readInput(const char *pPrompt, int numIndent) {
    memset(_cmdBuffer, 0, CMD_BUFFER_SIZE);
    // print tab
    for(int i = 0;i < numIndent;i++) {
        std::cout << TAB;
    }
    // print "wdb> "
    std::cout << pPrompt << "> ";
    // read a line from cmd
    readLine(_cmdBuffer, CMD_BUFFER_SIZE - 1);
    int curBufLen = strlen(_cmdBuffer);
    // "\" mens continue
    while(_cmdBuffer[curBufLen - 1] == BACK_SLANT && (CMD_BUFFER_SIZE - curBufLen) > 0) {
        for(int i = 0;i < numIndent;i++) {
            std::cout << TAB;
        }
        cout << "> ";
        readLine(&_cmdBuffer[curBufLen - 1], CMD_BUFFER_SIZE - curBufLen);
    }
    curBufLen = strlen(_cmdBuffer);
    for(int i = 0;i < curBufLen;i++) {
        if (_cmdBuffer[i] == TAB) {
            _cmdBuffer[i] = SPACE;
        }
    }
    //printf("redline finale return:%s\n", _cmdBuffer);
    return EDB_OK;
}

char *Edb::readLine(char *p, int length) {
    int len = 0;
    char ch;
    while((ch = getchar()) != NEW_LINE) {
        switch(ch) {
            case BACK_SLANT:
                break;
            default:
                p[len] = ch;
                len++;
        }
        //printf("ch = %c, p = %s\n", ch, p);
        continue;
    }
    len = strlen(p);
    p[len] = '\0';
    return p;
}

void Edb::split(const std::string &text, char delim, std::vector<std::string> &result) {
    size_t strLen = text.length();
    size_t first = 0;
    size_t pos = 0;
    for(first = 0;first < strLen;first = pos + 1) {
        pos = first;
        while(text[pos] != delim && pos < strLen) {
            pos++;
        }
        std::string str = text.substr(first, pos - first);
        result.push_back(str);
    }
    return;
}

int main(int argc, char** argv) {
   Edb edb;
   edb.start();
   return 0;
}