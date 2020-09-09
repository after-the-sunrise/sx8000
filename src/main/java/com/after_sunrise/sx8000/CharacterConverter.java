package com.after_sunrise.sx8000;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BaseConverter;

public class CharacterConverter extends BaseConverter<Character> {

	public CharacterConverter(String optionName) {
		super(optionName);
	}

	public Character convert(String value) {
		if (value.length() != 1) {
			throw new ParameterException(getErrorString(value, "an single char"));
		} else {
			return value.charAt(0);
		}
	}

}
