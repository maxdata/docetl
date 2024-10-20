import React, { createContext, useContext, useState, ReactNode, useEffect } from 'react';
import { Bookmark, BookmarkContextType, UserNote } from '@/app/types';

const BookmarkContext = createContext<BookmarkContextType | undefined>(undefined);

export const useBookmarkContext = () => {
  const context = useContext(BookmarkContext);
  if (!context) {
    throw new Error('useBookmarkContext must be used within a BookmarkProvider');
  }
  return context;
};

const BOOKMARKS_STORAGE_KEY = 'docetl_bookmarks';

export const BookmarkProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [bookmarks, setBookmarks] = useState<Bookmark[]>(() => {
    if (typeof window !== "undefined") {
      const storedBookmarks = localStorage.getItem(BOOKMARKS_STORAGE_KEY);
      return storedBookmarks ? JSON.parse(storedBookmarks) : [];
    }
    return [];
  });

  useEffect(() => {
    localStorage.setItem(BOOKMARKS_STORAGE_KEY, JSON.stringify(bookmarks));
  }, [bookmarks]);

  const addBookmark = (text: string, source: string, color: string, notes: UserNote[]) => {
    const newBookmark: Bookmark = {
      id: Date.now().toString(),
      text,
      source,
      color,
      notes
    };
    setBookmarks(prevBookmarks => [...prevBookmarks, newBookmark]);
  };

  const removeBookmark = (id: string) => {
    setBookmarks(prevBookmarks => prevBookmarks.filter(bookmark => bookmark.id !== id));
  };

  return (
    <BookmarkContext.Provider value={{ bookmarks, addBookmark, removeBookmark }}>
      {children}
    </BookmarkContext.Provider>
  );
};