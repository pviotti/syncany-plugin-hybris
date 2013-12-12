/*
 * Syncany, www.syncany.org
 * Copyright (C) 2011-2013 Philipp C. Heckel <philipp.heckel@gmail.com> 
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.syncany.daemon.websocket;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

/**
 * @author vincent
 *
 */
public class JsonHelper {
	public static String fromMapToString(Map<String, ?> params){
		String text = new Gson().toJson(params).toString();
		return text;
	}

	@SuppressWarnings("serial")
	public static Map<String, String> fromStringToMap(String s){
		Type mapType = new TypeToken<Map<String, String>>() {}.getType();
		Map<String, String> parameters = new Gson().fromJson(s, mapType);
		return parameters;
	}
}
