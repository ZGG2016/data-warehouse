package org.zgg.gmall.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> splitKeyword(String keyword) throws IOException {

        ArrayList<String> res = new ArrayList<>();

        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        Lexeme next = ikSegmenter.next();
        while(next != null){
            String word = next.getLexemeText();
            res.add(word);

            next = ikSegmenter.next();
        }

        return res;
    }

    public static void main(String[] args) throws IOException {
        List<String> keyword = splitKeyword("本季度工作概况");
        System.out.println(keyword);
    }
}
