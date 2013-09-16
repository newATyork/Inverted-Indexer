#include <string>
#include <vector>
#include <algorithm>   
#include <functional>   

#ifndef TRIM_H
#define TRIM_H

const std::string whiteSpaces( " \f\n\r\t\v" );

void trimRight( std::string& str,const std::string& trimChars = whiteSpaces );

void trimLeft( std::string& str, const std::string& trimChars = whiteSpaces );

void trim( std::string& str, const std::string& trimChars = whiteSpaces );

#endif  

